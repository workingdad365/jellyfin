using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Api.Controllers;
using Jellyfin.Api.Models;
using MediaBrowser.Common.Api;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Movies;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.IO;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Xunit;

namespace Jellyfin.Api.Tests.Controllers;

public sealed class TmdbPersonAliasesControllerTests
{
    private readonly Mock<ITmdbPersonAliasService> _aliases = new();
    private readonly Mock<ITmdbPersonSearchService> _people = new();
    private readonly Mock<ILibraryManager> _library = new();
    private readonly Mock<IProviderManager> _providers = new();

    public TmdbPersonAliasesControllerTests()
    {
        _people.Setup(people => people.GetPerson(123, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TmdbPersonCandidate { TmdbId = 123, Name = "Original Name" });
    }

    [Fact]
    public void Controller_RequiresAdministratorForEveryAction()
    {
        var authorization = typeof(TmdbPersonAliasesController).GetCustomAttribute<AuthorizeAttribute>();

        Assert.NotNull(authorization);
        Assert.Equal(Policies.RequiresElevation, authorization.Policy);
        Assert.All(
            typeof(TmdbPersonAliasesController).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly),
            method => Assert.Null(method.GetCustomAttribute<AllowAnonymousAttribute>()));
    }

    [Fact]
    public async Task SaveAlias_ValidRequestReturnsNoContent()
    {
        var result = await CreateController().SaveAlias(new TmdbPersonAliasDto { TmdbId = 123, Name = "Name" }, TestContext.Current.CancellationToken);

        Assert.IsType<NoContentResult>(result);
        _aliases.Verify(aliases => aliases.Save(123, "Name"), Times.Once);
    }

    [Fact]
    public async Task SaveAlias_NameCollisionReturnsConflict()
    {
        _aliases.Setup(aliases => aliases.Save(123, "Name")).Throws(new InvalidOperationException("Name collision"));

        var result = await CreateController().SaveAlias(new TmdbPersonAliasDto { TmdbId = 123, Name = "Name" }, TestContext.Current.CancellationToken);

        var response = Assert.IsType<ObjectResult>(result);
        Assert.Equal(StatusCodes.Status409Conflict, response.StatusCode);
        Assert.Equal("Name collision", Assert.IsType<ProblemDetails>(response.Value).Detail);
    }

    [Fact]
    public async Task SaveAlias_InvalidInputReturnsBadRequest()
    {
        _aliases.Setup(aliases => aliases.Save(123, " ")).Throws(new ArgumentException("Invalid name"));

        var result = await CreateController().SaveAlias(new TmdbPersonAliasDto { TmdbId = 123, Name = " " }, TestContext.Current.CancellationToken);

        Assert.Equal(StatusCodes.Status400BadRequest, Assert.IsType<ObjectResult>(result).StatusCode);
    }

    [Fact]
    public void DeleteAlias_DeletesOnlyRequestedId()
    {
        var result = CreateController().DeleteAlias(123);

        Assert.IsType<NoContentResult>(result);
        _aliases.Verify(aliases => aliases.Delete(123), Times.Once);
    }

    [Theory]
    [InlineData("Original Name")]
    [InlineData(" original name ")]
    public async Task SaveAlias_OriginalNameIsRejected(string name)
    {
        var result = await CreateController().SaveAlias(new TmdbPersonAliasDto { TmdbId = 123, Name = name }, TestContext.Current.CancellationToken);

        Assert.Equal(StatusCodes.Status400BadRequest, Assert.IsType<ObjectResult>(result).StatusCode);
        _aliases.Verify(aliases => aliases.Save(It.IsAny<int>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task SaveAlias_UnknownTmdbIdIsRejected()
    {
        var result = await CreateController().SaveAlias(new TmdbPersonAliasDto { TmdbId = 456, Name = "Name" }, TestContext.Current.CancellationToken);

        Assert.Equal(StatusCodes.Status400BadRequest, Assert.IsType<ObjectResult>(result).StatusCode);
        _aliases.Verify(aliases => aliases.Save(It.IsAny<int>(), It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task SearchPeople_PassesTrimmedNameAndPage()
    {
        _people.Setup(people => people.Search("정유미", 2, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TmdbPersonSearchResult());

        await CreateController().SearchPeople(" 정유미 ", 2, TestContext.Current.CancellationToken);

        _people.Verify(people => people.Search("정유미", 2, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public void RefreshItems_QueuesEachMatchingWorkOnceWithFullReplacement()
    {
        var first = new Movie { Id = Guid.NewGuid() };
        var second = new Movie { Id = Guid.NewGuid() };
        _library.Setup(library => library.GetItemList(It.Is<InternalItemsQuery>(query => query.Person == "Original Name" && query.Recursive == true)))
            .Returns(new BaseItem[] { first, second });
        _library.Setup(library => library.GetItemList(It.Is<InternalItemsQuery>(query => query.Person == "Previous Alias" && query.Recursive == true)))
            .Returns(new BaseItem[] { first });

        var result = CreateController().RefreshItems([" Original Name ", "Original Name", "Previous Alias"]);

        Assert.Equal(2, Assert.IsAssignableFrom<OkObjectResult>(result.Result).Value);
        foreach (var item in new[] { first, second })
        {
            _providers.Verify(
                providers => providers.QueueRefresh(
                    item.Id,
                    It.Is<MetadataRefreshOptions>(options =>
                        options.MetadataRefreshMode == MetadataRefreshMode.FullRefresh
                        && options.ImageRefreshMode == MetadataRefreshMode.FullRefresh
                        && options.ReplaceAllMetadata && options.ReplaceAllImages
                        && options.RemoveOldMetadata && options.ForceSave
                        && !options.IsAutomated && !options.RegenerateTrickplay),
                    RefreshPriority.High),
                Times.Once);
        }

        _library.Verify(library => library.GetItemList(It.Is<InternalItemsQuery>(query => query.Person == "Original Name")), Times.Once);
        _providers.VerifyNoOtherCalls();
    }

    [Fact]
    public void RefreshItems_NoMatchingWorksQueuesNothing()
    {
        _library.Setup(library => library.GetItemList(It.IsAny<InternalItemsQuery>())).Returns(Array.Empty<BaseItem>());

        var result = CreateController().RefreshItems(["Original Name"]);

        Assert.Equal(0, Assert.IsAssignableFrom<OkObjectResult>(result.Result).Value);
        _providers.VerifyNoOtherCalls();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("Invalid\nName")]
    public void RefreshItems_InvalidNameDoesNotRefreshEntireLibrary(string name)
    {
        var result = CreateController().RefreshItems(["Valid Name", name]);

        Assert.IsType<BadRequestResult>(result.Result);
        _library.VerifyNoOtherCalls();
        _providers.VerifyNoOtherCalls();
    }

    [Fact]
    public void RefreshItems_EmptyNamesDoesNotRefreshEntireLibrary()
    {
        var result = CreateController().RefreshItems([]);

        Assert.IsType<BadRequestResult>(result.Result);
        _library.VerifyNoOtherCalls();
        _providers.VerifyNoOtherCalls();
    }

    [Fact]
    public void RefreshItems_CollectsAllTargetsBeforeQueueing()
    {
        _library.Setup(library => library.GetItemList(It.Is<InternalItemsQuery>(query => query.Person == "First")))
            .Returns(new BaseItem[] { new Movie { Id = Guid.NewGuid() } });
        _library.Setup(library => library.GetItemList(It.Is<InternalItemsQuery>(query => query.Person == "Second")))
            .Throws(new InvalidOperationException("Query failed"));

        Assert.Throws<InvalidOperationException>(() => CreateController().RefreshItems(["First", "Second"]));

        _providers.VerifyNoOtherCalls();
    }

    private TmdbPersonAliasesController CreateController() => new(_aliases.Object, _people.Object, _library.Object, _providers.Object, Mock.Of<IFileSystem>())
    {
        ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext() }
    };
}
