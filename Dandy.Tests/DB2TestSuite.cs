using System;
using System.Data;
using IBM.Data.DB2.iSeries;

namespace Dandy.Tests
{
    public class DB2TestSuite : TestSuite
    {
        public static string Schema => "DAPPERTEST";
        public static string ConnectionString => $"PersistSecurityInfo=True;DataSource=AS400SIRE-DEV.ecor.local;UserID=RI_FRLA;Password=FRANCESCO;DefaultCollection={Schema}";
        public override IDbConnection GetConnection() => new iDB2Connection(ConnectionString);
        static DB2TestSuite()
        {
            using (var connection = new iDB2Connection(ConnectionString))
            {
                // ReSharper disable once AccessToDisposedClosure
                void dropTable(string name)
                {
                    var command = connection.CreateCommand();
                    command.CommandText = string.Format(

                    @"
                                      
                        DROP table  {0}.{1}
                     
                     ", Schema, name);
                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch (Exception) { }
                }
                void prepareLib()
                {
                    
                    runCommand($"ADDRPYLE SEQNBR(5000) MSGID(CPA7025) RPY(I) ");
                    runCommand($"CHGJOB INQMSGRPY(*SYSRPYL)");
                    runCommand($"DLTLIB ({Schema})");
                    runCommand($"RMVRPYLE SEQNBR(5000)");

                    runCommand($"CRTLIB ({Schema})");
                    runCommand($"CRTJRNRCV JRNRCV({Schema}/TESTJRNRCV)");
                    runCommand($"CRTJRN JRN({Schema}/TESTJRN) JRNRCV({Schema}/TESTJRNRCV)");
                    runCommand($"STRJRNLIB LIB({Schema}) JRN({Schema}/TESTJRN)");
                    runCommand($"ADDLIBLE ({Schema})");
                }
                void run(string commandText)
                {
                    var command = connection.CreateCommand();

                    command.CommandText = commandText;
                    command.ExecuteNonQuery();
                }
                void runCommand(string commandText)
                {
                    var command = connection.CreateCommand();
                    command.CommandText = $"CALL QCMDEXC ('{commandText}',{commandText.Length})";
                    command.ExecuteNonQuery();
                }

                connection.Open();
                prepareLib();
                dropTable("Stuff");
                run("CREATE  TABLE Stuff (TheId INTEGER GENERATED ALWAYS AS IDENTITY not null, Name varchar(100) not null, Created Date )");
                dropTable("People");
                run("CREATE  TABLE People (Id INTEGER GENERATED ALWAYS AS IDENTITY not null, Name varchar(100) not null)");
                dropTable("Users");
                run("CREATE  TABLE Users (Id INTEGER GENERATED ALWAYS AS IDENTITY not null, Name varchar(100) not null, Age integer not null)");
                dropTable("Automobiles");
                run("CREATE  TABLE Automobiles (Id INTEGER GENERATED ALWAYS AS IDENTITY not null, Name varchar(100) not null)");
                dropTable("Results");
                run("CREATE  TABLE Results (Id INTEGER GENERATED ALWAYS AS IDENTITY not null, Name varchar(100) not null, Order integer not null)");
                dropTable("ObjectX");
                run("CREATE  TABLE ObjectX (ObjectXId nvarchar(100) not null, Name varchar(100) not null)");
                dropTable("ObjectY");
                run("CREATE  TABLE ObjectY (ObjectYId integer not null, Name varchar(100) not null)");
                dropTable("ObjectZ");
                run("CREATE  TABLE ObjectZ (Id integer not null, Name varchar(100) not null)");
                dropTable("GenericType");
                run("CREATE  TABLE GenericTypes (Id nvarchar(100) not null, Name varchar(100) not null)");
                dropTable("NullableDates");
                run("CREATE  TABLE NullableDates (Id INTEGER GENERATED ALWAYS AS IDENTITY not null, DateValue date )");
                dropTable("Article");
                run("CREATE  TABLE Article (Id INTEGER GENERATED ALWAYS AS IDENTITY not null,Code varchar(15), Name varchar(100) not null) ");
                dropTable("TimeFields");
                run("CREATE  TABLE TimeFields (Id INTEGER GENERATED ALWAYS AS IDENTITY not null, TimeValue time )");
                dropTable("AliasedFields");
                run("CREATE  TABLE AliasedFields (IdRec INTEGER GENERATED ALWAYS AS IDENTITY not null, Field varchar(50))");
                dropTable("MultipleAliasedFields");
                run("CREATE  TABLE MultipleAliasedFields (IdARec INTEGER not null,IdBRec INTEGER not null, Field varchar(50))");
                dropTable("VYDEON0F");
                run("CREATE TABLE VYDEON0F(CPROVYD CHAR(30) DEFAULT ' ' NOT NULL, XPROVYD CHAR(30) DEFAULT ' ' NOT NULL, TDEC DATE DEFAULT '1940-01-01' NOT NULL, TOUT DATE DEFAULT '1940-01-01' NOT NULL, FL01VYD CHAR(1) DEFAULT ' ' NOT NULL, FL02VYD CHAR(1) DEFAULT ' ' NOT NULL, FL03VYD CHAR(1) DEFAULT ' ' NOT NULL, FL04VYD CHAR(1) DEFAULT ' ' NOT NULL, FL05VYD CHAR(1) DEFAULT ' ' NOT NULL, TDATVYD DATE DEFAULT '1940-01-01' NOT NULL, TIMEVYD TIME DEFAULT '00.00.00  ' NOT NULL, NAGR DECIMAL(5, 0) DEFAULT 0 NOT NULL, DAGR DATE DEFAULT '1940-01-01' NOT NULL, TAGR TIME DEFAULT '00.00.00  ' NOT NULL, UAGR CHAR(10) DEFAULT ' ' NOT NULL)");
            }
        }


    }
}
