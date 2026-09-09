using System;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Emby.Server.Implementations.Library;
using Jellyfin.Data.Enums;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using Microsoft.Data.Sqlite;
using Moq;
using Xunit;

namespace Jellyfin.Server.Implementations.Tests.Library;

public sealed class TmdbPersonAliasServiceTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
    private readonly Mock<IApplicationPaths> _paths = new();
    private readonly Mock<ILibraryManager> _library = new();
    private readonly TmdbPersonAliasService _service;

    public TmdbPersonAliasServiceTests()
    {
        _paths.SetupGet(paths => paths.DataPath).Returns(_directory);
        _library.Setup(library => library.GetPersonId(It.IsAny<string>()))
            .Returns((string name) => new Guid(SHA256.HashData(Encoding.UTF8.GetBytes(name.Trim().ToUpperInvariant())).AsSpan(0, 16)));
        _library.Setup(library => library.GetItemList(It.IsAny<InternalItemsQuery>())).Returns(Array.Empty<BaseItem>());
        _service = CreateService();
    }

    [Fact]
    public void Save_PersistsInSeparateDatabase()
    {
        _service.Save(123, "  Local Name  ");

        Assert.Equal("Local Name", CreateService().GetAliases()[123]);
        Assert.Equal("tmdb-person-aliases.db", Path.GetFileName(Assert.Single(Directory.GetFiles(_directory))));
    }

    [Fact]
    public void Save_UpdatesOneIdAndPreservesPreviousSnapshot()
    {
        _service.Save(123, "First Name");
        var snapshot = _service.GetAliases();
        _service.Save(123, "Second Name");

        Assert.Single(_service.GetAliases());
        Assert.Equal("Second Name", _service.GetAliases()[123]);
        Assert.Equal("First Name", snapshot[123]);
        Assert.Equal("First Name", _service.ResolveName(snapshot, 123, "Original Name"));
    }

    [Theory]
    [InlineData("Local Name")]
    [InlineData("local name")]
    [InlineData("  LOCAL NAME  ")]
    public void Save_DuplicateNameIsRejected(string name)
    {
        _service.Save(123, "Local Name");
        var snapshot = _service.GetAliases();

        Assert.Throws<InvalidOperationException>(() => _service.Save(456, name));
        Assert.Same(snapshot, _service.GetAliases());
        Assert.Single(CreateService().GetAliases());
    }

    [Fact]
    public void Save_DifferentNamesWithSameLibraryIdAreRejected()
    {
        var personId = Guid.NewGuid();
        _library.Setup(library => library.GetPersonId("First/Name")).Returns(personId);
        _library.Setup(library => library.GetPersonId("First_Name")).Returns(personId);
        _service.Save(123, "First/Name");

        Assert.Throws<InvalidOperationException>(() => _service.Save(456, "First_Name"));
    }

    [Fact]
    public void Save_UnicodeEquivalentNamesAreRejected()
    {
        _service.Save(123, "\u00e9");

        Assert.Throws<InvalidOperationException>(() => _service.Save(456, "e\u0301"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("456")]
    [InlineData("invalid")]
    public void Save_ExistingUnrelatedPersonIsRejected(string? existingTmdbId)
    {
        var person = new Person { Name = "Local Name" };
        if (existingTmdbId is not null)
        {
            person.SetProviderId(MetadataProvider.Tmdb, existingTmdbId);
        }

        _library.Setup(library => library.GetPerson("Local Name")).Returns(person);

        Assert.Throws<InvalidOperationException>(() => _service.Save(123, "Local Name"));
        Assert.Empty(_service.GetAliases());
    }

    [Fact]
    public void Save_ExistingDisplayNameWithDifferentInternalIdIsRejected()
    {
        var person = new Person { Name = "LOCAL NAME" };
        person.SetProviderId(MetadataProvider.Tmdb, "456");
        _library.Setup(library => library.GetItemList(It.Is<InternalItemsQuery>(query => query.Name == "Local Name")))
            .Returns(new BaseItem[] { person });

        Assert.Throws<InvalidOperationException>(() => _service.Save(123, "Local Name"));
    }

    [Fact]
    public void Save_ExistingSameTmdbPersonIsAllowed()
    {
        var person = new Person { Name = "Local Name" };
        person.SetProviderId(MetadataProvider.Tmdb, "123");
        _library.Setup(library => library.GetPerson("Local Name")).Returns(person);

        _service.Save(123, "Local Name");

        Assert.Equal("Local Name", _service.GetAliases()[123]);
    }

    [Theory]
    [InlineData(0, "Name")]
    [InlineData(-1, "Name")]
    [InlineData(1, "")]
    [InlineData(1, "   ")]
    [InlineData(1, "First\nLast")]
    [InlineData(1, ".")]
    [InlineData(1, "..")]
    public void Save_InvalidInputIsRejected(int tmdbId, string name)
    {
        Assert.ThrowsAny<ArgumentException>(() => _service.Save(tmdbId, name));
        Assert.False(Directory.Exists(_directory));
    }

    [Fact]
    public void Save_ExcessiveLengthIsRejected()
    {
        Assert.Throws<ArgumentException>(() => _service.Save(123, new string('a', 201)));
    }

    [Fact]
    public void Delete_PersistsWithoutMutatingSnapshot()
    {
        _service.Save(123, "Local Name");
        var snapshot = _service.GetAliases();

        _service.Delete(123);
        _service.Delete(123);

        Assert.Empty(CreateService().GetAliases());
        Assert.Equal("Local Name", snapshot[123]);
        Assert.Equal("Original Name", _service.ResolveName(_service.GetAliases(), 123, " Original Name "));
    }

    [Fact]
    public void ResolveName_ReservedNameCannotBeUsedByUnknownPerson()
    {
        _service.Save(123, "Local Name");

        Assert.Throws<InvalidOperationException>(() => _service.ResolveName(_service.GetAliases(), 456, " local name "));
        Assert.Equal("Unregistered", _service.ResolveName(_service.GetAliases(), 456, " Unregistered "));
    }

    [Fact]
    public void ResolveName_HomonymsRemainSeparateBeforeAddPerson()
    {
        _service.Save(123, "Name (first)");
        _service.Save(456, "Name (second)");
        var aliases = _service.GetAliases();
        var result = new MetadataResult<Person>();
        foreach (var tmdbId in new[] { 123, 456 })
        {
            var credit = new PersonInfo
            {
                Name = _service.ResolveName(aliases, tmdbId, "Name"),
                Type = PersonKind.Actor,
                Role = "Same role"
            };
            credit.SetProviderId(MetadataProvider.Tmdb, tmdbId.ToString(CultureInfo.InvariantCulture));
            result.AddPerson(credit);
        }

        Assert.Equal(2, result.People.Count);
        Assert.Equal("123", result.People[0].GetProviderId(MetadataProvider.Tmdb));
        Assert.Equal("456", result.People[1].GetProviderId(MetadataProvider.Tmdb));
    }

    [Fact]
    public void Save_DatabaseWriteFailureKeepsLastSnapshot()
    {
        _service.Save(123, "First Name");
        var snapshot = _service.GetAliases();
        var databasePath = Path.Combine(_directory, "tmdb-person-aliases.db");
        File.Move(databasePath, databasePath + ".backup");
        Directory.CreateDirectory(databasePath);

        Assert.Throws<SqliteException>(() => _service.Save(123, "Second Name"));
        Assert.Same(snapshot, _service.GetAliases());
        Assert.Equal("First Name", _service.GetAliases()[123]);
    }

    [Fact]
    public void GetAliases_CorruptDatabaseDoesNotFallBackToEmptyAliases()
    {
        Directory.CreateDirectory(_directory);
        File.WriteAllText(Path.Combine(_directory, "tmdb-person-aliases.db"), new string('x', 4096));

        Assert.Throws<SqliteException>(() => _service.GetAliases());
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }

    private TmdbPersonAliasService CreateService() => new(_paths.Object, _library.Object);
}
