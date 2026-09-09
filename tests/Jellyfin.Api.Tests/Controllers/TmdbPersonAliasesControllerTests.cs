using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Api.Controllers;
using Jellyfin.Api.Models;
using MediaBrowser.Common.Api;
using MediaBrowser.Controller.Providers;
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

    private TmdbPersonAliasesController CreateController() => new(_aliases.Object, _people.Object)
    {
        ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext() }
    };
}
