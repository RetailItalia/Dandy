using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Dandy.Mapping;
using System.Linq.Expressions;

namespace Dandy
{
    public static partial class SqlMapperExtensions
    {
        /// <summary>
        /// Returns a single entity by a single id from table "Ts" asynchronously using .NET 4.5 Task. T must be of interface type. 
        /// Id must be marked with [Key] attribute.
        /// Created entity is tracked/intercepted for changes and used by the Update() extension. 
        /// </summary>
        /// <typeparam name="T">Interface type to create and populate</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="id">Id of the entity to get, must be marked with [Key] attribute</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Entity of T</returns>
        public static async Task<T> GetAsync<T>(this IDbConnection connection, dynamic id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var type = typeof(T);
            var map = GetColumnAliasMap(type);
            var key = GetKeys<T>(nameof(GetAsync));
            var adapter = GetFormatter(connection);
            if (!GetQueries.TryGetValue(type.TypeHandle, out string sql))
            {
                var name = GetTableName(type);

                var allProperties = TypePropertiesCache(type);
                var computed = ComputedPropertiesCache(type);
                var sbColumnList = new StringBuilder();

                //var pars = key.Select(k => map.GetColumnName(k));

                allProperties.Except(computed).ToList().ForEach(_ =>
                {
                    adapter.AppendColumnName(sbColumnList, map.GetColumnName(_));

                    sbColumnList.Append(",");
                });
                sbColumnList = sbColumnList.Remove(sbColumnList.Length - 1, 1);

                sql = $"SELECT {sbColumnList.ToString()} FROM {name} WHERE {BuildWhereCondition(key,map)}";

                GetQueries[type.TypeHandle] = sql;
                GetParameters[type.TypeHandle] = key.Select(k => map.GetColumnName(k));
            }

            var dynParms = IsASystemType(id)
                ? BuildParametersWhereCondition(type.TypeHandle, id) as DynamicParameters
                : BuildParametersWhereCondition(type.TypeHandle, RemapObject(key, null, id)) as DynamicParameters;

            if (!type.IsInterface)
                return (await connection.QueryAsync<T>(sql, dynParms, transaction, commandTimeout).ConfigureAwait(false)).FirstOrDefault();

            var res = (await connection.QueryAsync<dynamic>(sql, dynParms).ConfigureAwait(false)).FirstOrDefault() as IDictionary<string, object>;

            if (res == null)
                return null;

            var obj = ProxyGenerator.GetInterfaceProxy<T>();

            foreach (var property in TypePropertiesCache(type))
            {
                var val = res[property.Name.ToUpper()];
                if (val == null) continue;
                if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    var genericType = Nullable.GetUnderlyingType(property.PropertyType);
                    if (genericType != null) property.SetValue(obj, Convert.ChangeType(val, genericType), null);
                }
                else
                {
                    property.SetValue(obj, Convert.ChangeType(val, property.PropertyType), null);
                }
            }

            ((IProxy)obj).IsDirty = false;   //reset change tracking and return

            return obj;
        }

        private static bool IsASystemType(dynamic id)
        {

            return id.GetType().Namespace != null && id.GetType().Namespace.StartsWith("System");
        }

        /// <summary>
        /// Returns a list of entites from table "Ts".  
        /// Id of T must be marked with [Key] attribute.
        /// Entities created from interfaces are tracked/intercepted for changes and used by the Update() extension
        /// for optimal performance. 
        /// </summary>
        /// <typeparam name="T">Interface or type to create and populate</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <param name="page">[At the moment it only works in DB2] page number (from 1 to ...)</param>
        /// <param name="pageSize">[At the moment it only works in DB2] page size (from 1 to ...)</param>
        /// /// <param name="filter">lambda expression where condigion</param>
        /// <returns>Entity of T</returns>
        public async static Task<IEnumerable<T>> GetAllAsync<T>(
            this IDbConnection connection,
            IDbTransaction transaction = null, int? commandTimeout = null, int? page = null, int? pageSize = null,
            Expression<Func<T, bool>> filter = null)
            where T : class
        {
            System.Diagnostics.Contracts.Contract.Requires((!pageSize.HasValue) || pageSize.HasValue && pageSize >= 0, "pageSize must be a number >= 0");
            var type = typeof(T);
            (string sql, DynamicParameters parameters) = BuildSqlGetAll(connection, filter);

            if (pageSize.HasValue)
            {
                if (parameters == null) parameters = new DynamicParameters();
                parameters.AddDynamicParams(GetPaginationParameters(pageSize, page));
                sql = AppendSqlWithPagination(connection, sql);
            }

            return !type.IsInterface ?
                await connection.QueryAsync<T>(sql, parameters, transaction, commandTimeout) :
                await GetAllAsyncImpl<T>(connection, parameters, transaction, commandTimeout, sql, type);
        }

        internal static (string sql, DynamicParameters parameters) BuildSqlGetAll<T>(IDbConnection connection, Expression<Func<T, bool>> filter = null)
        {
            var type = typeof(T);
            var adapter = GetFormatter(connection);
            var cacheType = typeof(List<T>);
            var map = GetColumnAliasMap(type);
            if (!GetQueries.TryGetValue(cacheType.TypeHandle, out string sql))
            {
                //GetSingleKey<T>(nameof(GetAll));

                var sbColumnList = new StringBuilder();
                var name = GetTableName(type);
                var allProperties = TypePropertiesCache(type);
                var computed = ComputedPropertiesCache(type);

                allProperties.Except(computed).ToList().ForEach(_ =>
                {
                    adapter.AppendColumnName(sbColumnList, map.GetColumnName(_));

                    sbColumnList.Append(",");
                });
                sbColumnList = sbColumnList.Remove(sbColumnList.Length - 1, 1);
                sql = $"select {sbColumnList.ToString()} from {name}";

                GetQueries[cacheType.TypeHandle] = sql;
            }
            if (filter != null)
            {
                var where = BuildWhere(filter, _ => map.GetColumnName(type.GetProperty(_)));
                sql += $" WHERE {where.SQL}";
                return (sql, where.Parameters);
            }
            return (sql, null);
        }

        public static BuildExpressionResult BuildWhere<T, TResult>(Expression<Func<T, TResult>> expression, Func<string, string> buildColumnName)
        {
            return new ExpressionBuilder(buildColumnName).Build(expression.Body);
        }
        private static string AppendSqlWithPagination(IDbConnection connection, string selectSql) =>
        (connection.GetType().Name.Contains("DB2")) ?
        $"{selectSql} LIMIT @top OFFSET @skip" :
        selectSql;

        private static DynamicParameters GetPaginationParameters(int? pageSize, int? page)
        {
            var pars = new DynamicParameters();
            pars.Add("top", pageSize.Value);
            pars.Add("skip", ((page ?? 1) - 1) * pageSize.Value);
            return pars;
        }

        private static async Task<IEnumerable<T>> GetAllAsyncImpl<T>(IDbConnection connection, object parameters, IDbTransaction transaction, int? commandTimeout, string sql, Type type) where T : class
        {
            var result = await connection.QueryAsync(sql, parameters).ConfigureAwait(false);
            var list = new List<T>();
            foreach (IDictionary<string, object> res in result)
            {
                var obj = ProxyGenerator.GetInterfaceProxy<T>();
                foreach (var property in TypePropertiesCache(type))
                {
                    var val = res[property.Name.ToUpper()];
                    if (val == null) continue;
                    if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        var genericType = Nullable.GetUnderlyingType(property.PropertyType);
                        if (genericType != null) property.SetValue(obj, Convert.ChangeType(val, genericType), null);
                    }
                    else
                    {
                        property.SetValue(obj, Convert.ChangeType(val, property.PropertyType), null);
                    }
                }
                ((IProxy)obj).IsDirty = false;   //reset change tracking and return
                list.Add(obj);
            }
            return list;
        }

        /// <summary>
        /// Inserts an entity into table "Ts" asynchronously using .NET 4.5 Task and returns identity id.
        /// </summary>
        /// <typeparam name="T">The type being inserted.</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <param name="sqlAdapter">The specific ISqlAdapter to use, auto-detected based on connection if null</param>
        /// <returns>Identity of inserted entity</returns>
        public static async Task<int> InsertAsync<T>(this IDbConnection connection, T entityToInsert, IDbTransaction transaction = null,
            int? commandTimeout = null, ISqlAdapter sqlAdapter = null) where T : class
        {
            var type = typeof(T);
            sqlAdapter = sqlAdapter ?? GetFormatter(connection);
            var map = GetColumnAliasMap(type);
            var isList = false;
            if (type.IsArray)
            {
                isList = true;
                type = type.GetElementType();
            }
            else if (type.IsGenericType)
            {
                var typeInfo = type.GetTypeInfo();
                bool implementsGenericIEnumerableOrIsGenericIEnumerable =
                    typeInfo.ImplementedInterfaces.Any(ti => ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) ||
                    typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);

                if (implementsGenericIEnumerableOrIsGenericIEnumerable)
                {
                    isList = true;
                    type = type.GetGenericArguments()[0];
                }
            }

            var name = GetTableName(type);
            var sbColumnList = new StringBuilder(null);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type).ToList();
            var computedProperties = ComputedPropertiesCache(type);
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
            {
                var property = allPropertiesExceptKeyAndComputed[i];
                sqlAdapter.AppendColumnName(sbColumnList, map.GetColumnName(property));
                if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                    sbColumnList.Append(", ");
            }

            var sbParameterList = new StringBuilder(null);
            for (var i = 0; i < allPropertiesExceptKeyAndComputed.Count; i++)
            {
                var property = allPropertiesExceptKeyAndComputed[i];
                sbParameterList.AppendFormat("@{0}", property.Name);
                if (i < allPropertiesExceptKeyAndComputed.Count - 1)
                    sbParameterList.Append(", ");
            }

            if (!isList)    //single entity
            {
                var id = await sqlAdapter.InsertAsync(connection, transaction, commandTimeout, name, sbColumnList.ToString(),
                    sbParameterList.ToString(), keyProperties,
                    RemapObject(keyProperties
                    , allPropertiesExceptKeyAndComputed
                    , entityToInsert));

                var propertyInfos = keyProperties.ToArray();
                if (propertyInfos.Length == 0) return id;

                var idProperty = propertyInfos[0];
                idProperty.SetValue(entityToInsert, Convert.ChangeType(id, idProperty.PropertyType), null);
                return id;

            }

            //insert list of entities
            var cmd = $"INSERT INTO {name} ({sbColumnList}) values ({sbParameterList})";
            return await connection.ExecuteAsync(cmd, entityToInsert, transaction, commandTimeout);
        }

        private static IAliasColumnMap GetColumnAliasMap(Type type) => ColumnNameMappingDictionary
                    .FirstOrDefault(_ => _.Key == type)
                    .Value as IAliasColumnMap
                    ?? new DefaultMap();

        private static IAliasTableMap GetTableAliasMap(Type type) => ColumnNameMappingDictionary
                    .FirstOrDefault(_ => _.Key == type)
                    .Value as IAliasTableMap
                    ?? new DefaultTableMap(type);

        /// <summary>
        /// Updates entity in table "Ts" asynchronously using .NET 4.5 Task, checks if the entity is modified if the entity is tracked by the Get() extension.
        /// </summary>
        /// <typeparam name="T">Type to be updated</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToUpdate">Entity to be updated</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if updated, false if not found or not modified (tracked entities)</returns>
        public static async Task<bool> UpdateAsync<T>(this IDbConnection connection, T entityToUpdate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            if ((entityToUpdate is IProxy proxy) && !proxy.IsDirty)
            {
                return false;
            }

            var type = typeof(T);
            var map = GetColumnAliasMap(type);

            if (type.IsArray)
            {
                type = type.GetElementType();
            }
            else if (type.IsGenericType)
            {
                var typeInfo = type.GetTypeInfo();
                bool implementsGenericIEnumerableOrIsGenericIEnumerable =
                    typeInfo.ImplementedInterfaces.Any(ti => ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) ||
                    typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);

                if (implementsGenericIEnumerableOrIsGenericIEnumerable)
                {
                    type = type.GetGenericArguments()[0];
                }
            }

            var keyProperties = KeyPropertiesCache(type).ToList();
            var explicitKeyProperties = ExplicitKeyPropertiesCache(type);
            if (keyProperties.Count == 0 && explicitKeyProperties.Count == 0)
                throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

            var name = GetTableName(type);

            var sb = new StringBuilder();
            sb.AppendFormat("update {0} set ", name);

            var allProperties = TypePropertiesCache(type);
            keyProperties.AddRange(explicitKeyProperties);
            var computedProperties = ComputedPropertiesCache(type);
            var nonIdProps = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var adapter = GetFormatter(connection);

            for (var i = 0; i < nonIdProps.Count; i++)
            {
                var property = nonIdProps[i];
                adapter.AppendColumnNameEqualsValue(sb, map.GetColumnName(property), property.Name);
                if (i < nonIdProps.Count - 1)
                    sb.Append(", ");
            }
            sb.Append(" where ");
            for (var i = 0; i < keyProperties.Count; i++)
            {
                var property = keyProperties[i];
                adapter.AppendColumnNameEqualsValue(sb, map.GetColumnName(property), property.Name);
                if (i < keyProperties.Count - 1)
                    sb.Append(" and ");
            }
            int updated = 0;


            if (entityToUpdate is System.Collections.IEnumerable enumerable)
            {
                var res = new List<IDictionary<string, object>>();
                foreach (var _ in enumerable)
                    res.Add(RemapObject(keyProperties, nonIdProps, _));
                updated = await connection.ExecuteAsync(sb.ToString(), res, commandTimeout: commandTimeout, transaction: transaction).ConfigureAwait(false);

            }
            else
            {
                updated = await connection.ExecuteAsync(sb.ToString()
                    , RemapObject(keyProperties, nonIdProps, entityToUpdate)
                    , commandTimeout: commandTimeout
                    , transaction: transaction).ConfigureAwait(false);
            }

            return updated > 0;
        }

        /// <summary>
        /// Delete entity in table "Ts" asynchronously using .NET 4.5 Task.
        /// </summary>
        /// <typeparam name="T">Type of entity</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToDelete">Entity to delete</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if deleted, false if not found</returns>
        public static async Task<bool> DeleteAsync<T>(this IDbConnection connection, T entityToDelete, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            if (entityToDelete == null)
                throw new ArgumentException("Cannot Delete null Object", nameof(entityToDelete));

            var type = typeof(T);
            var map = GetColumnAliasMap(type);

            if (type.IsArray)
            {
                type = type.GetElementType();
            }
            else if (type.IsGenericType)
            {
                var typeInfo = type.GetTypeInfo();
                bool implementsGenericIEnumerableOrIsGenericIEnumerable =
                    typeInfo.ImplementedInterfaces.Any(ti => ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) ||
                    typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);

                if (implementsGenericIEnumerableOrIsGenericIEnumerable)
                {
                    type = type.GetGenericArguments()[0];
                }
            }

            var keyProperties = KeyPropertiesCache(type);
            var explicitKeyProperties = ExplicitKeyPropertiesCache(type);
            if (keyProperties.Count == 0 && explicitKeyProperties.Count == 0)
                throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

            var name = GetTableAliasMap(type).GetTableMap();
            keyProperties.AddRange(explicitKeyProperties);

            var sb = new StringBuilder();
            sb.AppendFormat("DELETE FROM {0} WHERE ", name);

            var adapter = GetFormatter(connection);

            for (var i = 0; i < keyProperties.Count; i++)
            {
                var property = keyProperties[i];
                adapter.AppendColumnNameEqualsValue(sb, map.GetColumnName(property), property.Name);
                if (i < keyProperties.Count - 1)
                    sb.Append(" AND ");
            }

            var deleted = 0;


            if (entityToDelete is System.Collections.IEnumerable enumerable)
            {
                var res = new List<IDictionary<string, object>>();
                foreach (var _ in enumerable)
                    res.Add(RemapObject(keyProperties, null, _));
                deleted = await connection.ExecuteAsync(sb.ToString(), res, commandTimeout: commandTimeout, transaction: transaction).ConfigureAwait(false);

            }
            else
            {
                deleted = await connection.ExecuteAsync(sb.ToString()
                    , RemapObject(keyProperties, null, entityToDelete)
                    , commandTimeout: commandTimeout
                    , transaction: transaction).ConfigureAwait(false);
            }


            return deleted > 0;
        }

        /// <summary>
        /// Delete all entities in the table related to the type T asynchronously using .NET 4.5 Task.
        /// </summary>
        /// <typeparam name="T">Type of entity</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if deleted, false if none found</returns>
        public static async Task<bool> DeleteAllAsync<T>(this IDbConnection connection, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var type = typeof(T);
            var statement = "DELETE FROM " + GetTableAliasMap(type).GetTableMap();
            var deleted = await connection.ExecuteAsync(statement, null, transaction, commandTimeout).ConfigureAwait(false);
            return deleted > 0;
        }
    }


}

public partial interface ISqlAdapter
{
    /// <summary>
    /// Inserts <paramref name="entityToInsert"/> into the database, returning the Id of the row created.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    /// <param name="transaction">The transaction to use.</param>
    /// <param name="commandTimeout">The command timeout to use.</param>
    /// <param name="tableName">The table to insert into.</param>
    /// <param name="columnList">The columns to set with this insert.</param>
    /// <param name="parameterList">The parameters to set for this insert.</param>
    /// <param name="keyProperties">The key columns in this table.</param>
    /// <param name="entityToInsert">The entity to insert.</param>
    /// <returns>The Id of the row created.</returns>
    Task<int> InsertAsync(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert);
}

public partial class SqlServerAdapter
{
    /// <summary>
    /// Inserts <paramref name="entityToInsert"/> into the database, returning the Id of the row created.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    /// <param name="transaction">The transaction to use.</param>
    /// <param name="commandTimeout">The command timeout to use.</param>
    /// <param name="tableName">The table to insert into.</param>
    /// <param name="columnList">The columns to set with this insert.</param>
    /// <param name="parameterList">The parameters to set for this insert.</param>
    /// <param name="keyProperties">The key columns in this table.</param>
    /// <param name="entityToInsert">The entity to insert.</param>
    /// <returns>The Id of the row created.</returns>
    public async Task<int> InsertAsync(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
    {
        var cmd = $"INSERT INTO {tableName} ({columnList}) values ({parameterList}); SELECT SCOPE_IDENTITY() id";
        var multi = await connection.QueryMultipleAsync(cmd, entityToInsert, transaction, commandTimeout).ConfigureAwait(false);

        var first = multi.Read().FirstOrDefault();
        if (first == null || first.id == null) return 0;

        var id = (int)first.id;
        var pi = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (pi.Length == 0) return id;

        var idp = pi[0];
        idp.SetValue(entityToInsert, Convert.ChangeType(id, idp.PropertyType), null);

        return id;
    }
}

public partial class SqlCeServerAdapter
{
    /// <summary>
    /// Inserts <paramref name="entityToInsert"/> into the database, returning the Id of the row created.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    /// <param name="transaction">The transaction to use.</param>
    /// <param name="commandTimeout">The command timeout to use.</param>
    /// <param name="tableName">The table to insert into.</param>
    /// <param name="columnList">The columns to set with this insert.</param>
    /// <param name="parameterList">The parameters to set for this insert.</param>
    /// <param name="keyProperties">The key columns in this table.</param>
    /// <param name="entityToInsert">The entity to insert.</param>
    /// <returns>The Id of the row created.</returns>
    public async Task<int> InsertAsync(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
    {
        var cmd = $"INSERT INTO {tableName} ({columnList}) VALUES ({parameterList})";
        await connection.ExecuteAsync(cmd, entityToInsert, transaction, commandTimeout).ConfigureAwait(false);
        var r = (await connection.QueryAsync<dynamic>("SELECT @@IDENTITY id", transaction: transaction, commandTimeout: commandTimeout).ConfigureAwait(false)).ToList();

        if (r[0] == null || r[0].id == null) return 0;
        var id = (int)r[0].id;

        var pi = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (pi.Length == 0) return id;

        var idp = pi[0];
        idp.SetValue(entityToInsert, Convert.ChangeType(id, idp.PropertyType), null);

        return id;
    }
}

public partial class MySqlAdapter
{
    /// <summary>
    /// Inserts <paramref name="entityToInsert"/> into the database, returning the Id of the row created.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    /// <param name="transaction">The transaction to use.</param>
    /// <param name="commandTimeout">The command timeout to use.</param>
    /// <param name="tableName">The table to insert into.</param>
    /// <param name="columnList">The columns to set with this insert.</param>
    /// <param name="parameterList">The parameters to set for this insert.</param>
    /// <param name="keyProperties">The key columns in this table.</param>
    /// <param name="entityToInsert">The entity to insert.</param>
    /// <returns>The Id of the row created.</returns>
    public async Task<int> InsertAsync(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName,
        string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
    {
        var cmd = $"INSERT INTO {tableName} ({columnList}) VALUES ({parameterList})";
        await connection.ExecuteAsync(cmd, entityToInsert, transaction, commandTimeout).ConfigureAwait(false);
        var r = await connection.QueryAsync<dynamic>("SELECT LAST_INSERT_ID() id", transaction: transaction, commandTimeout: commandTimeout).ConfigureAwait(false);

        var id = r.First().id;
        if (id == null) return 0;
        var pi = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (pi.Length == 0) return Convert.ToInt32(id);

        var idp = pi[0];
        idp.SetValue(entityToInsert, Convert.ChangeType(id, idp.PropertyType), null);

        return Convert.ToInt32(id);
    }
}

public partial class PostgresAdapter
{
    /// <summary>
    /// Inserts <paramref name="entityToInsert"/> into the database, returning the Id of the row created.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    /// <param name="transaction">The transaction to use.</param>
    /// <param name="commandTimeout">The command timeout to use.</param>
    /// <param name="tableName">The table to insert into.</param>
    /// <param name="columnList">The columns to set with this insert.</param>
    /// <param name="parameterList">The parameters to set for this insert.</param>
    /// <param name="keyProperties">The key columns in this table.</param>
    /// <param name="entityToInsert">The entity to insert.</param>
    /// <returns>The Id of the row created.</returns>
    public async Task<int> InsertAsync(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
    {
        var sb = new StringBuilder();
        sb.AppendFormat("INSERT INTO {0} ({1}) VALUES ({2})", tableName, columnList, parameterList);

        // If no primary key then safe to assume a join table with not too much data to return
        var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (propertyInfos.Length == 0)
        {
            sb.Append(" RETURNING *");
        }
        else
        {
            sb.Append(" RETURNING ");
            bool first = true;
            foreach (var property in propertyInfos)
            {
                if (!first)
                    sb.Append(", ");
                first = false;
                sb.Append(property.Name);
            }
        }

        var results = await connection.QueryAsync(sb.ToString(), entityToInsert, transaction, commandTimeout).ConfigureAwait(false);

        // Return the key by assinging the corresponding property in the object - by product is that it supports compound primary keys
        var id = 0;
        foreach (var p in propertyInfos)
        {
            var value = ((IDictionary<string, object>)results.First())[p.Name.ToLower()];
            p.SetValue(entityToInsert, value, null);
            if (id == 0)
                id = Convert.ToInt32(value);
        }
        return id;
    }
}

public partial class SQLiteAdapter
{
    /// <summary>
    /// Inserts <paramref name="entityToInsert"/> into the database, returning the Id of the row created.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    /// <param name="transaction">The transaction to use.</param>
    /// <param name="commandTimeout">The command timeout to use.</param>
    /// <param name="tableName">The table to insert into.</param>
    /// <param name="columnList">The columns to set with this insert.</param>
    /// <param name="parameterList">The parameters to set for this insert.</param>
    /// <param name="keyProperties">The key columns in this table.</param>
    /// <param name="entityToInsert">The entity to insert.</param>
    /// <returns>The Id of the row created.</returns>
    public async Task<int> InsertAsync(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
    {
        var cmd = $"INSERT INTO {tableName} ({columnList}) VALUES ({parameterList}); SELECT last_insert_rowid() id";
        var multi = await connection.QueryMultipleAsync(cmd, entityToInsert, transaction, commandTimeout).ConfigureAwait(false);

        var id = (int)multi.Read().First().id;
        var pi = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        if (pi.Length == 0) return id;

        var idp = pi[0];
        idp.SetValue(entityToInsert, Convert.ChangeType(id, idp.PropertyType), null);

        return id;
    }
}

public partial class FbAdapter
{
    /// <summary>
    /// Inserts <paramref name="entityToInsert"/> into the database, returning the Id of the row created.
    /// </summary>
    /// <param name="connection">The connection to use.</param>
    /// <param name="transaction">The transaction to use.</param>
    /// <param name="commandTimeout">The command timeout to use.</param>
    /// <param name="tableName">The table to insert into.</param>
    /// <param name="columnList">The columns to set with this insert.</param>
    /// <param name="parameterList">The parameters to set for this insert.</param>
    /// <param name="keyProperties">The key columns in this table.</param>
    /// <param name="entityToInsert">The entity to insert.</param>
    /// <returns>The Id of the row created.</returns>
    public async Task<int> InsertAsync(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
    {
        var cmd = $"insert into {tableName} ({columnList}) values ({parameterList})";
        await connection.ExecuteAsync(cmd, entityToInsert, transaction, commandTimeout).ConfigureAwait(false);

        var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
        var keyName = propertyInfos[0].Name;
        var r = await connection.QueryAsync($"SELECT FIRST 1 {keyName} ID FROM {tableName} ORDER BY {keyName} DESC", transaction: transaction, commandTimeout: commandTimeout).ConfigureAwait(false);

        var id = r.First().ID;
        if (id == null) return 0;
        if (propertyInfos.Length == 0) return Convert.ToInt32(id);

        var idp = propertyInfos[0];
        idp.SetValue(entityToInsert, Convert.ChangeType(id, idp.PropertyType), null);

        return Convert.ToInt32(id);
    }
}

public class BuildExpressionResult
{
    public BuildExpressionResult(string sql, DynamicParameters @params)
    {
        SQL = sql;
        Parameters = @params;
    }
    public string SQL { get; }
    public DynamicParameters Parameters { get; }
}
public class ExpressionBuilder
{
    private const string concatOperator = "||";
    private int _index = 0;
    private readonly DynamicParameters _parameters = new DynamicParameters();

    private int GetNextIndex() => _index++;

    private readonly Func<string, string> _buildColumnName;
    public ExpressionBuilder(Func<string, string> buildColumnName)
    {
        _buildColumnName = buildColumnName;
    }

    public BuildExpressionResult Build(Expression expr) =>
        new BuildExpressionResult(PerformBuild(expr).ToString(), _parameters);

    private object PerformBuild(Expression expr)
    {
        switch (expr)
        {
            case MethodCallExpression mce:
                return ParseMethodCallExpr(mce);
            case MemberExpression me:
                var val = ParseMemberExpr(me);
                return (val.ToString());
            case ConstantExpression ce:
                return ParseConstantExpr(ce);
            case UnaryExpression ue:
                {
                    if (ue.Operand is BinaryExpression)
                    {
                        var parsed = ProcessTreeNode(ue.Operand as BinaryExpression);

                        return $"{GetOperator(ue)}({parsed})";
                    }
                    return $"{GetOperator(ue)}({PerformBuild(ue.Operand)})";
                }
            case BinaryExpression be:
                return ProcessTreeNode(be);
            default: return string.Empty;
        }
    }

    private string ProcessTreeNode(BinaryExpression be)
    {
        var x = PerformBuild(be.Left);
        var y = PerformBuild(be.Right);

        if (IsValue(be.Left))
            return AddParameter(y.ToString(), x, GetOperator(be));
        if (IsValue(be.Right))
            return AddParameter(x.ToString(), y, GetOperator(be));

        return $"{x} {GetOperator(be)} {y}";
    }

    private string AddParameter(string left, object right, string operatorName)
    {
        var columnName = $"@{left}_{GetNextIndex()}";
        _parameters.Add(columnName, right);
        return $"{left} {operatorName} {columnName}";
    }

    private bool IsValue(Expression expression) =>
        expression is ConstantExpression ||
            ((expression is MemberExpression) && (expression as MemberExpression).Expression.NodeType != ExpressionType.Parameter);

    private static string GetOperator(Expression be)
    {
        switch (be.NodeType)
        {
            case ExpressionType.Not: return "NOT";
            case ExpressionType.Equal: return "=";
            case ExpressionType.NotEqual: return "<>";
            case ExpressionType t when
            t == ExpressionType.AndAlso || t == ExpressionType.And:
                return "AND";
            case ExpressionType t when
            t == ExpressionType.OrElse || t == ExpressionType.Or:
                return "OR";
            case ExpressionType.GreaterThan: return ">";
            case ExpressionType.GreaterThanOrEqual: return ">=";
            case ExpressionType.LessThan: return "<";
            case ExpressionType.LessThanOrEqual: return "<=";
            default: return string.Empty;
        }
    }

    private string ParseMethodCallExpr(MethodCallExpression me)
    {
        var columnName = $"{ParseMemberExpr(me.Object as MemberExpression)}";
        var parColumnName = $"@{columnName}_{GetNextIndex()}";
        object value = CompileExpression(me.Arguments.FirstOrDefault());
        switch (me.Method.Name)
        {
            case "Contains":
                _parameters.Add(parColumnName.ToString(), value);
                return ($"UCASE({columnName}) LIKE '%'{concatOperator}UCASE({parColumnName}){concatOperator}'%'");
            case "StartsWith":
                _parameters.Add(parColumnName.ToString(), value);
                return ($"UCASE({columnName}) LIKE UCASE({parColumnName}){concatOperator}'%'");
            case "EndsWith":
                _parameters.Add(parColumnName.ToString(), value);
                return ($"UCASE({columnName}) LIKE '%'{concatOperator}UCASE({parColumnName})");
            default: throw new Exception($"{me.Method.Name} is not supported");
        }
    }
    private object ParseMemberExpr(MemberExpression me) =>
        me.Expression.NodeType != ExpressionType.Parameter ?
        ParseExpr(me.Expression, me.Member.Name) :
        _buildColumnName(me.Member.Name);

    private object ParseExpr(Expression e, string memberName) =>
        ParseValue(CompileExpression(e, memberName));

    private object CompileExpression(Expression e, string memberName)
    {
        var c = CompileExpression(e);
        return CompileExpression(Expression.PropertyOrField(Expression.Constant(c), memberName));
    }
    private static object CompileExpression(Expression e) => Expression.Lambda(e).Compile().DynamicInvoke();

    private static object ParseConstantExpr(ConstantExpression ce) =>
        ParseValue(ce.Value);
    private static object ParseValue(object value)
    {
        if (value is null)
            return "NULL";
        if (value is string)
            return value.ToString();
        return value;
    }
}