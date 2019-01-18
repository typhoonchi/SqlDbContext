using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;

namespace TyphoonChi
{
    public class SqlDbContext:IDisposable
    {
        private string _connectString;
        private string _database;
        private SqlConnection _sqlConnection;
        private SqlCommand _sqlCommand;
        private SqlDataAdapter _sqlDataAdapter;
        private SqlTransaction _sqlTransaction;
        private SqlBulkCopy _sqlBulkCopy;
        private bool _disposed = false;
        
        public string ConnectString { get => _connectString; set => _connectString = value; }
        public string Database { get => _database; }
        public string CommandText{ get => _sqlCommand.CommandText; set => _sqlCommand.CommandText = value; }

        public SqlDbContext()
        {
#if DWZQ_IPO
            _connectString = "Data Source=10.15.1.72;Initial Catalog=analysis;" +
                "Integrated Security=False;User ID=sa;Password=scs;Connect Timeout=30;" +
                "Encrypt=False;TrustServerCertificate=True;";
#else
            _connectString = "Data Source=(localdb)\\MSSQLLocalDB;"
                    + "Initial Catalog=master;"
                    + "Integrated Security=True;";
#endif
            _sqlConnection = new SqlConnection(_connectString);
            _database = _sqlConnection.Database;
            _sqlCommand = new SqlCommand()
            {
                Connection = _sqlConnection
            };
            _sqlBulkCopy = new SqlBulkCopy(_sqlConnection, SqlBulkCopyOptions.Default, _sqlTransaction);
            _sqlDataAdapter = new SqlDataAdapter();
        }

        public SqlDbContext(string sqlConnectString)
        {
            _connectString = sqlConnectString;            
            _sqlConnection = new SqlConnection(_connectString);
            _database = _sqlConnection.Database;
            _sqlCommand = new SqlCommand()
            {
                Connection = _sqlConnection
            };            
        }

#region IDisposable Support Code
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        public void Close()
        {
            _sqlConnection.Close();
        }

        ~SqlDbContext()
        {
            Dispose(false);
        }

        virtual protected void Dispose(bool disposing)
        {
            if(_disposed)
            {
                return;
            }
            if(disposing)
            {
                //ToDo:Dispose managed resource
            }
            _sqlConnection.Close();
            _sqlConnection.Dispose();
            _sqlDataAdapter.Dispose();
            _sqlBulkCopy.Close();
            _sqlTransaction?.Dispose();
            //MessageBox.Show("调用了Dispose()");
            _disposed = true;
        }
#endregion

        public void ChangeDatabase(string database)
        {
            if (_sqlConnection.State == ConnectionState.Closed) 
            {
                OpenConnection();
            }
            _sqlConnection.ChangeDatabase(database);
            this._database = database;
        }

        public bool TryConnect()
        {
            ConnectionState state = _sqlConnection.State;
            if (state != ConnectionState.Closed)
                return true;
            try
            {
                OpenConnection();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                if (state == ConnectionState.Closed)
                {
                    _sqlConnection.Close();
                }
            }
        }

        public void OpenConnection()
        {
            if (_sqlConnection.State == ConnectionState.Closed)
            {
                _sqlConnection.Open();
            }
        }

        public void FillData(DataSet dataSet)
        {
            OpenConnection();
            if (_sqlDataAdapter == null)
            {
                _sqlDataAdapter = new SqlDataAdapter();
            }
            _sqlDataAdapter.SelectCommand = _sqlCommand;
            _sqlDataAdapter.Fill(dataSet);
        }

        public void FillData(DataTable dataTable)
        {
            OpenConnection();
            if (_sqlDataAdapter == null)
            {
                _sqlDataAdapter = new SqlDataAdapter();
            }
            _sqlDataAdapter.SelectCommand = _sqlCommand;
            _sqlDataAdapter.Fill(dataTable);
        }

        public bool UpdateDataSet(DataSet dataSet,string tableName)
        {
            if (dataSet == null || tableName == string.Empty)
                return false;
            if (!dataSet.HasChanges())
                return false;
            
            if (_sqlDataAdapter == null)
            {
                _sqlDataAdapter = new SqlDataAdapter();
            }
            try
            {
                OpenConnection();
                _sqlCommand.CommandText = $"select * from {tableName};";
                _sqlDataAdapter.SelectCommand = _sqlCommand;
                SqlCommandBuilder scb = new SqlCommandBuilder(_sqlDataAdapter);
                _sqlDataAdapter.Update(dataSet.GetChanges());
                dataSet.AcceptChanges();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public void AddParameterWithValue(string parameterName,object value)
        {
            _sqlCommand.Parameters.AddWithValue(parameterName, value);
        }

        public void BeginTransaction()
        {
            OpenConnection();
            _sqlTransaction = _sqlConnection.BeginTransaction();
            _sqlCommand.Transaction = _sqlTransaction;
        }

        public void CommitTransaction()
        {
            _sqlTransaction.Commit();
        }

        public void RollbackTransaction()
        {
            _sqlTransaction.Rollback();
        }

        public object ExecuteScalar()
        {
            OpenConnection();
            return _sqlCommand.ExecuteScalar();
        }

        public object ExecuteScalar(string commandText)
        {
            OpenConnection();
            _sqlCommand.CommandText = commandText;
            return _sqlCommand.ExecuteScalar();
        }

        public int ExecuteNonQuery()
        {
            OpenConnection();
            return _sqlCommand.ExecuteNonQuery();
        }

        public int ExecuteNonQuery(string commandText)
        {
            OpenConnection();
            _sqlCommand.CommandText = commandText;
            return _sqlCommand.ExecuteNonQuery();
        }

        public SqlDataReader ExecuteReader()
        {
            OpenConnection();
            return _sqlCommand.ExecuteReader();
        }

        public SqlDataReader ExecuteReader(string commandText)
        {
            OpenConnection();
            _sqlCommand.CommandText = commandText;
            return _sqlCommand.ExecuteReader();
        }

        public void BulkCopy(DataTable dataTable, string tableName,int batchSize=1000)
        {
            if(_sqlBulkCopy==null)
            {
                _sqlBulkCopy = new SqlBulkCopy(_sqlConnection,SqlBulkCopyOptions.Default,_sqlTransaction);
            }
            _sqlBulkCopy.BatchSize = batchSize; 
            _sqlBulkCopy.NotifyAfter = batchSize;
            _sqlBulkCopy.DestinationTableName = tableName;
            _sqlBulkCopy.WriteToServer(dataTable);
        }

        public bool DatabaseExists(string databaseName)
        {
            if (databaseName == string.Empty || databaseName == null)
                return false;
            OpenConnection();
            _sqlCommand.CommandText = "select count(*) from sysdatabases where name=@databaseName";
            AddParameterWithValue("@databaseName", databaseName);
            return (int)ExecuteScalar() > 0 ? true : false;
        }

        public bool TableExists(string tableName)
        {
            if (tableName == string.Empty || tableName == null)
                return false;
            OpenConnection();
            _sqlCommand.CommandText = $"select count(*) from sysobjects where id=object_id('{tableName}');";
            return (int)ExecuteScalar() > 0 ? true : false;
        }

        public bool DropTable(string tableName)
        {
            if (tableName == string.Empty || tableName == null)
                return false;
            OpenConnection();
            _sqlCommand.CommandText= $"if object_id('{tableName}') is not null drop table {tableName};";
            ExecuteNonQuery();
            return true;
        }

        public bool ClearTable(string tableName)
        {
            if (tableName == string.Empty || tableName == null)
                return false;
            OpenConnection();
            _sqlCommand.CommandText = $"if object_id('{tableName}') is not null truncate table {tableName};";
            ExecuteNonQuery();
            return true;
        }

        public List<string> GetUserTableList()
        {
            OpenConnection();
            DataTable dt = new DataTable();
            _sqlCommand.CommandText = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES";
            FillData(dt);
            var query = from p in dt.AsEnumerable()
                        select p.Field<string>("TABLE_NAME");
            return query.ToList();
        }
    }
}
