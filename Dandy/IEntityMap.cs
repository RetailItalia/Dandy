using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Dapper;

namespace Dandy.Mapping
{
    public interface IAliasPropertyMap
    {
        PropertyInfo GetPropertyMap(string name);
    }
    public interface IAliasColumnMap
    {
        string GetColumnName(PropertyInfo property);
    }

    public interface IAliasTableMap
    {
        string GetTableMap();
    }

    public interface IEntityMap : IAliasColumnMap, IAliasPropertyMap, IAliasTableMap { };
    public interface IEntityMap<T> : IEntityMap { };

    public class PropertyMap
    {
        public string ColumnName { get; set; }
        public PropertyInfo Field { get; set; }
        public void ToColumn(string name) => ColumnName = name;
    }

    public class DefaultMap : IAliasColumnMap
    {
        public string GetColumnName(PropertyInfo property) => property.Name;

    }

    public class DefaultTableMap : IAliasTableMap
    {
        private readonly Type type;

        public DefaultTableMap(Type type)
        {
            
            this.type = type;
        }
        public string GetTableMap()
        {
            if (type.IsGenericType)
                return $"{type.Name.Remove(type.Name.Length - 2)}s";
            if (type.IsInterface && type.Name.StartsWith("I"))
                return $"{type.Name.Substring(1)}s";

            return $"{type.Name}s";
        }
    }

    public class EntityMap<T> : IEntityMap<T> where T : class
    {
        IList<PropertyMap> PropertyMaps { get; }
        private string Table { get; set; }
        public EntityMap()
        {
            Table = new DefaultTableMap(typeof(T)).GetTableMap();
            PropertyMaps = new List<PropertyMap>();
        }

        protected void ToTable(string name)
        {
            Table = name;
        }

        protected PropertyMap Map(Expression<Func<T, object>> expression)
        {


            var member = ReflectionHelper.GetMemberInfo((LambdaExpression)expression);


            var name = member.Name;
            var memberType = expression.Parameters[0].Type;

            var map = new PropertyMap
            {
                ColumnName = name,
                Field = memberType.GetProperty(name)
            };
            PropertyMaps.Add(map);
            return map;

        }

        public string GetColumnName(PropertyInfo property) => PropertyMaps.FirstOrDefault(_ => _.Field == property)?.ColumnName ?? property.Name;

        public PropertyInfo GetPropertyMap(string name) => PropertyMaps.FirstOrDefault(_ => _.ColumnName.Equals(name, StringComparison.CurrentCultureIgnoreCase))?.Field;

        public string GetTableMap() => Table ;
    }
}
