using System;
using System.Linq;
using Emby.Server.Implementations.Data;
using Jellyfin.Data.Enums;
using Jellyfin.Database.Implementations;
using Jellyfin.Database.Implementations.Entities;
using Jellyfin.Server.Implementations.Item;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Persistence;
using Moq;
using Xunit;
using BaseItemKind = Jellyfin.Data.Enums.BaseItemKind;

namespace Jellyfin.Server.Implementations.Tests.Item;

public sealed class PeopleRepositoryUpdatePeopleTests : SqliteDbTestFixture
{
    private static readonly Guid _itemId = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    private readonly PeopleRepository _repository;

    public PeopleRepositoryUpdatePeopleTests()
    {
        var itemTypeLookup = new ItemTypeLookup();

        using (var ctx = CreateDbContext())
        {
            ctx.BaseItems.Add(new BaseItemEntity
            {
                Id = _itemId,
                Type = itemTypeLookup.BaseItemKindNames[BaseItemKind.Movie],
                Name = "Movie",
                MediaType = "Video",
                IsMovie = true,
                IsFolder = false,
                IsVirtualItem = false
            });
            ctx.SaveChanges();
        }

        _repository = new PeopleRepository(
            CreateDbContextFactory(),
            itemTypeLookup,
            new Mock<IItemQueryHelpers>().Object);
    }

    [Fact]
    public void UpdatePeople_SamePersonAndTypeWithDifferentRoles_KeepsEveryCredit()
    {
        _repository.UpdatePeople(_itemId, [
            CreatePerson("Person A", PersonKind.Writer, "Novel"),
            CreatePerson("Person A", PersonKind.Writer, "Screenplay")
        ]);

        using var ctx = CreateDbContext();
        Assert.Single(ctx.Peoples);
        Assert.Equal(
            ["Novel", "Screenplay"],
            ctx.PeopleBaseItemMap.OrderBy(e => e.ListOrder).Select(e => e.Role ?? string.Empty).ToArray());
    }

    [Fact]
    public void UpdatePeople_CreditsDifferingOnlyInCase_AreDeduped()
    {
        _repository.UpdatePeople(_itemId, [
            CreatePerson("Person A", PersonKind.Actor, "Hero"),
            CreatePerson("person a", PersonKind.Actor, "hero")
        ]);

        using var ctx = CreateDbContext();
        Assert.Single(ctx.Peoples);
        var map = Assert.Single(ctx.PeopleBaseItemMap);
        Assert.Equal("Hero", map.Role);
    }

    [Fact]
    public void UpdatePeople_SamePersonAsDifferentTypes_CreatesOnePersonPerType()
    {
        _repository.UpdatePeople(_itemId, [
            CreatePerson("Person A", PersonKind.Actor, "Hero"),
            CreatePerson("Person A", PersonKind.Director, string.Empty)
        ]);

        using var ctx = CreateDbContext();
        Assert.Equal(2, ctx.Peoples.Count());
        Assert.Equal(2, ctx.PeopleBaseItemMap.Count());
    }

    [Fact]
    public void UpdatePeople_RepeatedUpdate_ReusesMappingsAndRefreshesOrder()
    {
        _repository.UpdatePeople(_itemId, [
            CreatePerson("Person A", PersonKind.Actor, "Hero"),
            CreatePerson("Person B", PersonKind.Actor, "Sidekick")
        ]);

        Guid[] peopleIdsBefore;
        using (var ctx = CreateDbContext())
        {
            peopleIdsBefore = ctx.Peoples.Select(e => e.Id).OrderBy(e => e).ToArray();
        }

        // Reversed order, so the list order of both mappings has to be rewritten.
        _repository.UpdatePeople(_itemId, [
            CreatePerson("Person B", PersonKind.Actor, "Sidekick"),
            CreatePerson("Person A", PersonKind.Actor, "Hero")
        ]);

        using var after = CreateDbContext();
        Assert.Equal(peopleIdsBefore, after.Peoples.Select(e => e.Id).OrderBy(e => e).ToArray());
        Assert.Equal(
            ["Sidekick", "Hero"],
            after.PeopleBaseItemMap.OrderBy(e => e.ListOrder).Select(e => e.Role ?? string.Empty).ToArray());
    }

    [Fact]
    public void UpdatePeople_CreditRemoved_DropsOnlyThatMapping()
    {
        _repository.UpdatePeople(_itemId, [
            CreatePerson("Person A", PersonKind.Writer, "Novel"),
            CreatePerson("Person A", PersonKind.Writer, "Screenplay")
        ]);

        _repository.UpdatePeople(_itemId, [
            CreatePerson("Person A", PersonKind.Writer, "Novel")
        ]);

        using var ctx = CreateDbContext();
        var map = Assert.Single(ctx.PeopleBaseItemMap);
        Assert.Equal("Novel", map.Role);
    }

    [Fact]
    public void UpdatePeople_RoleCaseChanged_KeepsExistingMapping()
    {
        _repository.UpdatePeople(_itemId, [CreatePerson("Person A", PersonKind.Actor, "Hero")]);

        _repository.UpdatePeople(_itemId, [CreatePerson("Person A", PersonKind.Actor, "HERO")]);

        using var ctx = CreateDbContext();
        var map = Assert.Single(ctx.PeopleBaseItemMap);
        Assert.Equal("Hero", map.Role);
    }

    [Fact]
    public void UpdatePeople_CreditDroppedByTheProvider_LeavesNoCreditRowBehind()
    {
        _repository.UpdatePeople(_itemId, [
            CreatePerson("Person A", PersonKind.Actor, "Hero"),
            CreatePerson("Person B", PersonKind.Actor, "Villain")
        ]);

        _repository.UpdatePeople(_itemId, [CreatePerson("Person A", PersonKind.Actor, "Hero")]);

        using var ctx = CreateDbContext();
        Assert.Equal(["Person A"], ctx.Peoples.Select(e => e.Name).ToArray());
    }

    [Fact]
    public void UpdatePeople_CreditStillHeldByAnotherItem_IsKept()
    {
        _repository.UpdatePeople(_itemId, [CreatePerson("Person A", PersonKind.Actor, "Hero")]);
        _repository.UpdatePeople(AddMovie("Other Movie"), [CreatePerson("Person A", PersonKind.Actor, "Hero")]);

        _repository.UpdatePeople(_itemId, []);

        using var after = CreateDbContext();
        Assert.Single(after.Peoples);
        Assert.Single(after.PeopleBaseItemMap);
    }

    [Fact]
    public void DeleteOrphanedCredits_CreditNoItemMapsTo_IsDeleted()
    {
        _repository.UpdatePeople(_itemId, [CreatePerson("Person A", PersonKind.Actor, "Hero")]);
        using (var ctx = CreateDbContext())
        {
            // The state a credit was left in before UpdatePeople cleaned up after itself.
            ctx.PeopleBaseItemMap.RemoveRange(ctx.PeopleBaseItemMap);
            ctx.SaveChanges();
        }

        Assert.Equal(1, _repository.DeleteOrphanedCredits());

        using var after = CreateDbContext();
        Assert.Empty(after.Peoples);
    }

    [Fact]
    public void DeleteOrphanedCredits_CreditAnItemMapsTo_IsKept()
    {
        _repository.UpdatePeople(_itemId, [CreatePerson("Person A", PersonKind.Actor, "Hero")]);

        Assert.Equal(0, _repository.DeleteOrphanedCredits());

        using var after = CreateDbContext();
        Assert.Single(after.Peoples);
    }

    [Fact]
    public void GetPeople_UnmappedOriginalNameIsExcludedBeforePaging()
    {
        _repository.UpdatePeople(_itemId, [CreatePerson("황정민", PersonKind.Actor, "Hero")]);
        using (var context = CreateDbContext())
        {
            context.PeopleBaseItemMap.RemoveRange(context.PeopleBaseItemMap);
            context.SaveChanges();
        }

        _repository.UpdatePeople(_itemId, [
            CreatePerson("황정민 (남자)", PersonKind.Actor, "Hero"),
            CreatePerson("황정민 (여자)", PersonKind.Actor, "Other")
        ]);

        var result = _repository.GetPeople(new InternalPeopleQuery
        {
            NameContains = "황정민",
            EnableTotalRecordCount = true,
            StartIndex = 0,
            Limit = 1
        });
        var nextPage = _repository.GetPeople(new InternalPeopleQuery
        {
            NameContains = "황정민",
            EnableTotalRecordCount = true,
            StartIndex = 1,
            Limit = 1
        });

        Assert.Equal(2, result.TotalRecordCount);
        Assert.Equal("황정민 (남자)", Assert.Single(result.Items).Name);
        Assert.Equal(2, nextPage.TotalRecordCount);
        Assert.Equal("황정민 (여자)", Assert.Single(nextPage.Items).Name);
        using var after = CreateDbContext();
        Assert.Equal(3, after.Peoples.Count());
    }

    [Fact]
    public void GetPeople_OriginalNameStillCreditedOnAnItemIsKept()
    {
        _repository.UpdatePeople(_itemId, [CreatePerson("황정민", PersonKind.Actor, "Hero")]);
        _repository.UpdatePeople(AddMovie("Refreshed Movie"), [
            CreatePerson("황정민 (남자)", PersonKind.Actor, "Hero"),
            CreatePerson("황정민 (여자)", PersonKind.Actor, "Other")
        ]);

        var result = _repository.GetPeople(new InternalPeopleQuery
        {
            NameContains = "황정민",
            EnableTotalRecordCount = true
        });

        Assert.Equal(3, result.TotalRecordCount);
        Assert.Contains(result.Items, person => person.Name == "황정민");
    }

    [Fact]
    public void GetPeople_OnlyUnmappedCreditsReturnsEmptyResults()
    {
        _repository.UpdatePeople(_itemId, [CreatePerson("Original Name", PersonKind.Actor, "Hero")]);
        using (var context = CreateDbContext())
        {
            context.PeopleBaseItemMap.RemoveRange(context.PeopleBaseItemMap);
            context.SaveChanges();
        }

        var result = _repository.GetPeople(new InternalPeopleQuery { EnableTotalRecordCount = true });

        Assert.Equal(0, result.TotalRecordCount);
        Assert.Empty(result.Items);
    }

    private Guid AddMovie(string name)
    {
        var id = Guid.NewGuid();
        using var ctx = CreateDbContext();
        ctx.BaseItems.Add(new BaseItemEntity
        {
            Id = id,
            Type = new ItemTypeLookup().BaseItemKindNames[BaseItemKind.Movie],
            Name = name,
            MediaType = "Video",
            IsMovie = true,
            IsFolder = false,
            IsVirtualItem = false
        });
        ctx.SaveChanges();
        return id;
    }

    private static PersonInfo CreatePerson(string name, PersonKind type, string role)
    {
        return new PersonInfo
        {
            Name = name,
            Type = type,
            Role = role
        };
    }
}
