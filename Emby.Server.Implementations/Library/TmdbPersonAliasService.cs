using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Jellyfin.Data.Enums;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using Microsoft.Data.Sqlite;

namespace Emby.Server.Implementations.Library;

/// <summary>
/// 별도 SQLite 파일에 TMDB 인물 별칭을 보관한다.
/// </summary>
public sealed class TmdbPersonAliasService : ITmdbPersonAliasService
{
    private readonly object _lock = new();
    private readonly string _databasePath;
    private readonly ILibraryManager _libraryManager;
    private IReadOnlyDictionary<int, string>? _aliases;

    /// <summary>
    /// 별칭 저장소를 초기화한다. 파일은 최초 조회 또는 수정 시 생성한다.
    /// </summary>
    /// <param name="applicationPaths">별도 DB 파일을 보관할 데이터 경로.</param>
    /// <param name="libraryManager">기존 인물의 이름 기반 식별자를 조회할 관리자.</param>
    public TmdbPersonAliasService(IApplicationPaths applicationPaths, ILibraryManager libraryManager)
    {
        _databasePath = Path.Combine(applicationPaths.DataPath, "tmdb-person-aliases.db");
        _libraryManager = libraryManager;
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<int, string> GetAliases()
    {
        lock (_lock)
        {
            if (_aliases is not null)
            {
                return _aliases;
            }

            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT TmdbId, Name FROM PersonAliases";
            using var reader = command.ExecuteReader();
            var aliases = new Dictionary<int, string>();
            while (reader.Read())
            {
                aliases.Add(reader.GetInt32(0), reader.GetString(1));
            }

            _aliases = aliases.ToFrozenDictionary();
            return _aliases;
        }
    }

    /// <inheritdoc />
    public string ResolveName(IReadOnlyDictionary<int, string> aliases, int tmdbId, string originalName)
    {
        if (aliases.TryGetValue(tmdbId, out var name))
        {
            return name;
        }

        name = originalName.Trim();
        if (aliases.Count > 0)
        {
            var personId = _libraryManager.GetPersonId(name);
            var normalizedName = name.Normalize(NormalizationForm.FormC);
            if (aliases.Any(alias => string.Equals(alias.Value, normalizedName, StringComparison.OrdinalIgnoreCase)
                || _libraryManager.GetPersonId(alias.Value) == personId))
            {
                throw new InvalidOperationException($"TMDB person {tmdbId} conflicts with a reserved person alias. Configure a distinct alias before refreshing metadata.");
            }
        }

        return name;
    }

    /// <inheritdoc />
    public void Save(int tmdbId, string name)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tmdbId);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        name = name.Trim().Normalize(NormalizationForm.FormC);
        if (name.Length > 200 || name.Any(char.IsControl) || string.IsNullOrWhiteSpace(name.TrimEnd('.')))
        {
            throw new ArgumentException("인물명은 제어 문자 없이 200자 이하여야 하며, 점으로만 구성할 수 없습니다.", nameof(name));
        }

        lock (_lock)
        {
            var aliases = GetAliases();
            var personId = _libraryManager.GetPersonId(name);
            if (aliases.Any(alias => alias.Key != tmdbId
                && (string.Equals(alias.Value, name, StringComparison.OrdinalIgnoreCase)
                    || _libraryManager.GetPersonId(alias.Value) == personId)))
            {
                throw new InvalidOperationException("다른 TMDB 인물이 이미 사용 중인 별칭입니다.");
            }

            var existing = _libraryManager.GetPerson(name);
            var sameNamePeople = _libraryManager.GetItemList(new InternalItemsQuery
            {
                IncludeItemTypes = [BaseItemKind.Person],
                Name = name
            });
            if ((existing is not null && IsDifferentPerson(existing, tmdbId))
                || sameNamePeople.Any(person => string.Equals(person.Name?.Normalize(NormalizationForm.FormC), name, StringComparison.OrdinalIgnoreCase)
                    && IsDifferentPerson(person, tmdbId)))
            {
                throw new InvalidOperationException("기존 라이브러리의 다른 인물 또는 TMDB ID가 없는 인물과 이름이 충돌합니다.");
            }

            var updated = aliases.ToDictionary(alias => alias.Key, alias => alias.Value);
            updated[tmdbId] = name;
            var snapshot = updated.ToFrozenDictionary();
            using var connection = OpenConnection();
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO PersonAliases (TmdbId, Name, NameKey)
                VALUES ($id, $name, $key)
                ON CONFLICT(TmdbId) DO UPDATE SET Name = excluded.Name, NameKey = excluded.NameKey
                """;
            command.Parameters.AddWithValue("$id", tmdbId);
            command.Parameters.AddWithValue("$name", name);
            command.Parameters.AddWithValue("$key", personId.ToString("N"));
            command.ExecuteNonQuery();
            transaction.Commit();
            _aliases = snapshot;
        }
    }

    /// <inheritdoc />
    public void Delete(int tmdbId)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tmdbId);
        lock (_lock)
        {
            var updated = GetAliases().ToDictionary(alias => alias.Key, alias => alias.Value);
            updated.Remove(tmdbId);
            var snapshot = updated.ToFrozenDictionary();
            using var connection = OpenConnection();
            using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM PersonAliases WHERE TmdbId = $id";
            command.Parameters.AddWithValue("$id", tmdbId);
            command.ExecuteNonQuery();
            _aliases = snapshot;
        }
    }

    private static bool IsDifferentPerson(BaseItem person, int tmdbId)
    {
        return !int.TryParse(person.GetProviderId(MetadataProvider.Tmdb), NumberStyles.Integer, CultureInfo.InvariantCulture, out var existingTmdbId)
            || existingTmdbId != tmdbId;
    }

    private SqliteConnection OpenConnection()
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_databasePath)!);
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = _databasePath,
            Pooling = false
        }.ToString());
        try
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = """
                CREATE TABLE IF NOT EXISTS PersonAliases (
                    TmdbId INTEGER PRIMARY KEY CHECK (TmdbId > 0),
                    Name TEXT NOT NULL CHECK (length(trim(Name)) BETWEEN 1 AND 200),
                    NameKey TEXT NOT NULL UNIQUE
                )
                """;
            command.ExecuteNonQuery();
            return connection;
        }
        catch (SqliteException)
        {
            connection.Dispose();
            throw;
        }
    }
}
