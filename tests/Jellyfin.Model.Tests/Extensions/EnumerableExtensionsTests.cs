using System.Collections.Generic;
using System.Linq;
using MediaBrowser.Model.Extensions;
using MediaBrowser.Model.Providers;
using Xunit;

namespace Jellyfin.Model.Tests.Extensions;

public class EnumerableExtensionsTests
{
    [Fact]
    public void OrderByLanguageDescending_WhenPreferredLanguageMissing_PrioritizesEnglishBeforeNoLanguage()
    {
        var images = new List<RemoteImageInfo>
        {
            new()
            {
                Url = "no-language",
                Language = null,
                CommunityRating = 10,
                VoteCount = 100
            },
            new()
            {
                Url = "english",
                Language = "en",
                CommunityRating = 1,
                VoteCount = 1
            }
        };

        var ordered = images.OrderByLanguageDescending("ko").ToList();

        Assert.Equal("english", ordered[0].Url);
        Assert.Equal("no-language", ordered[1].Url);
    }

    [Fact]
    public void OrderByLanguageDescending_WhenPreferredLanguageExists_PrioritizesPreferredLanguage()
    {
        var images = new List<RemoteImageInfo>
        {
            new()
            {
                Url = "english",
                Language = "en"
            },
            new()
            {
                Url = "korean",
                Language = "ko"
            }
        };

        var ordered = images.OrderByLanguageDescending("ko").ToList();

        Assert.Equal("korean", ordered[0].Url);
    }
}
