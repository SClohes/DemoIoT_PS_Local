#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.Store;
using FTOptix.SQLiteStore;
using FTOptix.DataLogger;
using FTOptix.HMIProject;
using FTOptix.NetLogic;
using FTOptix.Core;
using FTOptix.ODBCStore;
using System.Security.Cryptography.X509Certificates;
using System.Collections.Generic;
using System.Linq;
#endregion
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
using System.Net.Security;
using System.Threading;
using static uPLibrary.Networking.M2Mqtt.MqttClient;
using CloudConnector;
using Newtonsoft.Json;
using System.Globalization;
using System.Text;
using System.IO;
using FTOptix.Recipe;
using FTOptix.CommunicationDriver;
using FTOptix.Retentivity;
using FTOptix.NativeUI;
using FTOptix.Modbus;
using FTOptix.EthernetIP;

namespace CloudConnector
{
    public abstract class Record
    {
        public Record(DateTime? timestamp)
        {
            this.timestamp = timestamp;
        }

        public override bool Equals(object obj)
        {
            var other = obj as Record;
            return timestamp == other.timestamp;
        }

        public readonly DateTime? timestamp;
    }

    public class DataLoggerRecord : Record
    {
        public DataLoggerRecord(DateTime timestamp, List<VariableRecord> variables) : base(timestamp)
        {
            this.variables = variables;
        }

        public override bool Equals(object obj)
        {
            DataLoggerRecord other = obj as DataLoggerRecord;

            if (other == null)
                return false;

            if (timestamp != other.timestamp)
                return false;

            if (variables.Count != other.variables.Count)
                return false;

            for (int i = 0; i < variables.Count; ++i)
            {
                if (!variables[i].Equals(other.variables[i]))
                    return false;
            }

            return true;
        }

        public readonly List<VariableRecord> variables;
    }

    public class VariableRecord : Record
    {
        public VariableRecord(DateTime? timestamp,
                              string variableId,
                              UAValue value,
                              string serializedValue) : base(timestamp)
        {
            this.variableId = variableId;
            this.value = value;
            this.serializedValue = serializedValue;
            this.variableOpCode = null;
        }

        public VariableRecord(DateTime? timestamp,
                              string variableId,
                              UAValue value,
                              string serializedValue,
                              int? variableOpCode) : base(timestamp)
        {
            this.variableId = variableId;
            this.value = value;
            this.serializedValue = serializedValue;
            this.variableOpCode = variableOpCode;
        }

        public override bool Equals(object obj)
        {
            var other = obj as VariableRecord;
            return timestamp == other.timestamp &&
                   variableId == other.variableId &&
                   value == other.value &&
                   serializedValue == other.serializedValue &&
                   variableOpCode == other.variableOpCode;
        }

        public readonly string variableId;
        public readonly string serializedValue;
        public readonly UAValue value;
        public readonly int? variableOpCode;
    }

    public class Packet
    {
        public Packet(DateTime timestamp, string clientId)
        {
            this.timestamp = timestamp.ToUniversalTime();
            this.clientId = clientId;
        }

        public readonly DateTime timestamp;
        public readonly string clientId;
    }

    public class VariablePacket : Packet
    {
        public VariablePacket(DateTime timestamp,
                              string clientId,
                              List<VariableRecord> records) : base(timestamp, clientId)
        {
            this.records = records;
        }

        public readonly List<VariableRecord> records;
    }

    public class DataLoggerRowPacket : Packet
    {
        public DataLoggerRowPacket(DateTime timestamp,
                                   string clientId,
                                   List<DataLoggerRecord> records) : base(timestamp, clientId)
        {
            this.records = records;
        }

        public readonly List<DataLoggerRecord> records;
    }

    public class DataLoggerRecordUtils
    {
        public static List<DataLoggerRecord> GetDataLoggerRecordsFromQueryResult(object[,] resultSet, string[] header, List<VariableToLog> variablesToLogList, bool insertOpCode, bool insertVariableTimestamp)
        {
            var records = new List<DataLoggerRecord>();

            var rowCount = resultSet != null ? resultSet.GetLength(0) : 0;
            var columnCount = header != null ? header.Length : 0;
            for (int i = 0; i < rowCount; ++i)
            {
                var rowVariables = new List<VariableRecord>();
                DateTime rowTimestamp = GetTimestamp(resultSet[i, 0]);
                int variableIndex = 0;
                for (int j = 1; j < columnCount;)
                {
                    string variableId = header[j];
                    object value = resultSet[i, j];
                    string serializedValue = SerializeValue(value, variablesToLogList[variableIndex]);

                    DateTime? timestamp = null;
                    if (insertVariableTimestamp)
                    {
                        ++j; // Consume timestamp column
                        var timestampColumnValue = resultSet[i, j];
                        if (timestampColumnValue != null)
                            timestamp = GetTimestamp(timestampColumnValue);
                    }

                    VariableRecord variableRecord;
                    if (insertOpCode)
                    {
                        ++j; // Consume operation code column
                        var opCodeColumnValue = resultSet[i, j];
                        int? opCode = (opCodeColumnValue != null) ? (Int32.Parse(resultSet[i, j].ToString())) : (int?)null;
                        variableRecord = new VariableRecord(timestamp, variableId, GetUAValue(value, variablesToLogList[variableIndex]), serializedValue, opCode);
                    }
                    else
                        variableRecord = new VariableRecord(timestamp, variableId, GetUAValue(value, variablesToLogList[variableIndex]), serializedValue);

                    rowVariables.Add(variableRecord);

                    ++j; // Consume Variable Column
                    ++variableIndex;
                }

                var record = new DataLoggerRecord(rowTimestamp, rowVariables);
                records.Add(record);
            }

            return records;
        }

        private static string SerializeValue(object value, VariableToLog variableToLog)
        {
            if (value == null)
                return null;
            var valueType = variableToLog.ActualDataType;
            if (valueType == OpcUa.DataTypes.DateTime)
                return (GetTimestamp(value)).ToString("O");
            else if (valueType == OpcUa.DataTypes.Float)
                return ((float)((double)value)).ToString("G9");
            else if (valueType == OpcUa.DataTypes.Double)
                return ((double)value).ToString("G17");

            return value.ToString();
        }

        private static UAValue GetUAValue(object value, VariableToLog variableToLog)
        {
            if (value == null)
                return null;
            try
            {
                NodeId valueType = variableToLog.ActualDataType;
                if (valueType == OpcUa.DataTypes.Boolean)
                    return new UAValue(Int32.Parse(GetBoolean(value)));
                else if (valueType == OpcUa.DataTypes.Integer)
                    return new UAValue(Int64.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.UInteger)
                    return new UAValue(UInt64.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.Byte)
                    return new UAValue(Byte.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.SByte)
                    return new UAValue(SByte.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.Int16)
                    return new UAValue(Int16.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.UInt16)
                    return new UAValue(UInt16.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.Int32)
                    return new UAValue(Int32.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.UInt32)
                    return new UAValue(UInt32.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.Int64)
                    return new UAValue(Int64.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.UInt64)
                    return new UAValue(UInt64.Parse(value.ToString()));
                else if (valueType == OpcUa.DataTypes.Float)
                    return new UAValue((float)((double)value));
                else if (valueType == OpcUa.DataTypes.Double)
                    return new UAValue((double)value);
                else if (valueType == OpcUa.DataTypes.DateTime)
                    return new UAValue(GetTimestamp(value));
                else if (valueType == OpcUa.DataTypes.String)
                    return new UAValue(value.ToString());
                else if (valueType == OpcUa.DataTypes.ByteString)
                    return new UAValue((ByteString)value);
                else if (valueType == OpcUa.DataTypes.NodeId)
                    return new UAValue((NodeId)value);
            }
            catch (Exception e)
            {
                Log.Warning("PushAgent", "Parse Exception: " + e.Message);
                throw;
            }

            return null;
        }

        private static string GetBoolean(object value)
        {
            var valueString = value.ToString();
            if (valueString == "0" || valueString == "1")
                return valueString;

            if (valueString.ToLower() == "false")
                return "0";
            else
                return "1";
        }

        private static DateTime GetTimestamp(object value)
        {
            if (Type.GetTypeCode(value.GetType()) == TypeCode.DateTime)
                return ((DateTime)value);
            else
                return DateTime.SpecifyKind(DateTime.Parse(value.ToString()), DateTimeKind.Utc);
        }
    }

    public class DataLoggerStoreWrapper
    {
        public DataLoggerStoreWrapper(Store store,
                                      string tableName,
                                      List<VariableToLog> variablesToLogList,
                                      bool insertOpCode,
                                      bool insertVariableTimestamp,
                                      bool preserveDataLoggerHistory)
        {
            this.store = store;
            this.tableName = tableName;
            this.variablesToLogList = variablesToLogList;
            this.insertOpCode = insertOpCode;
            this.insertVariableTimestamp = insertVariableTimestamp;
            this.preserveDataLoggerHistory = preserveDataLoggerHistory;
        }

        public void DeletePulledRecords()
        {
            if (store.Status == StoreStatus.Offline)
                return;

            try
            {
                string query = $"DELETE FROM \"{tableName}\" AS D " +
                               $"WHERE \"Id\" IN " +
                               $"( SELECT \"Id\" " +
                               $"FROM \"##tempDataLoggerTable\")";

                store.Query(query, out _, out _);
            }
            catch (Exception e)
            {
                throw new Exception("Failed to delete from DataLogger temporary table " + e.Message);
            }

            DeleteTemporaryTable();
        }

        public List<DataLoggerRecord> QueryNewEntries()
        {
            if (store.Status == StoreStatus.Offline)
                return new List<DataLoggerRecord>();

            CopyNewEntriesToTemporaryTable();
            List<DataLoggerRecord> records = QueryNewEntriesFromTemporaryTable();

            if (records.Count == 0)
                DeleteTemporaryTable();

            return records;
        }

        public List<DataLoggerRecord> QueryNewEntriesUsingLastQueryId(UInt64 rowId)
        {
            if (store.Status == StoreStatus.Offline)
                return new List<DataLoggerRecord>();

            CopyNewEntriesToTemporaryTableUsingId(rowId);
            List<DataLoggerRecord> records = QueryNewEntriesFromTemporaryTable();

            if (records.Count == 0)
                DeleteTemporaryTable();

            return records;
        }

        public UInt64? GetMaxIdFromTemporaryTable()
        {
            object[,] resultSet;

            try
            {
                string query = $"SELECT MAX(\"Id\") FROM \"##tempDataLoggerTable\"";

                if (store.Status == StoreStatus.Online)
                {
                    store.Query(query, out _, out resultSet);
                    DeleteTemporaryTable();

                    if (resultSet[0, 0] != null)
                        return UInt64.Parse(resultSet[0, 0].ToString());
                }

                return null;
            }
            catch (Exception e)
            {
                Log.Error("PushAgent", "Failed to query maxid from DataLogger temporary table: " + e.Message);
                throw;
            }
        }

        public UInt64? GetDataLoggerMaxId()
        {
            object[,] resultSet;

            try
            {
                string query = $"SELECT MAX(\"Id\") FROM \"{tableName}\"";
                if (store.Status == StoreStatus.Online)
                {
                    store.Query(query, out _, out resultSet);

                    if (resultSet[0, 0] != null)
                        return UInt64.Parse(resultSet[0, 0].ToString());
                }

                return null;
            }
            catch (Exception e)
            {
                Log.Error("PushAgent", "Failed to query maxid from DataLogger temporary table: " + e.Message);
                throw;
            }
        }

        public StoreStatus GetStoreStatus()
        {
            if (store == null)
                return StoreStatus.Error;

            return store.Status;
        }

        private void CopyNewEntriesToTemporaryTable()
        {
            try
            {
                string query = $"CREATE TEMPORARY TABLE \"##tempDataLoggerTable\" AS " +
                               $"SELECT * " +
                               $"FROM \"{tableName}\" " +
                               $"WHERE \"Id\" IS NOT NULL " +
                               $"ORDER BY \"Timestamp\" ASC ";

                if (store.Status == StoreStatus.Online)
                    store.Query(query, out _, out _);
            }
            catch (Exception e)
            {
                throw new Exception("Failed to create internal temporary table: " + e.Message);
            }
        }

        private void CopyNewEntriesToTemporaryTableUsingId(UInt64 rowId)
        {
            try
            {
                Int64 id = rowId == Int64.MaxValue ? -1 : (Int64)rowId; // -1 to consider also id = 0
                string query = $"CREATE TEMPORARY TABLE \"##tempDataLoggerTable\" AS " +
                               $"SELECT * " +
                               $"FROM \"{tableName}\" " +
                               $"WHERE \"Id\" > {id} " +
                               $"ORDER BY \"Timestamp\" ASC ";

                if (store.Status == StoreStatus.Online)
                    store.Query(query, out _, out _);
            }
            catch (Exception e)
            {
                throw new Exception("Failed to create internal temporary table: " + e.Message);
            }
        }

        private void DeleteTemporaryTable()
        {
            object[,] resultSet;
            string[] header;

            try
            {
                string query = $"DROP TABLE \"##tempDataLoggerTable\"";
                store.Query(query, out header, out resultSet);
            }
            catch (Exception e)
            {
                throw new Exception("Failed to delete internal temporary table: " + e.Message);
            }
        }

        private List<DataLoggerRecord> QueryNewEntriesFromTemporaryTable()
        {
            List<DataLoggerRecord> records = null;
            object[,] resultSet;
            string[] header;

            try
            {
                string query = $"SELECT \"Timestamp\", {GetQueryColumnsOrderedByVariableName()} " +
                               $"FROM \"##tempDataLoggerTable\"";

                if (store.Status == StoreStatus.Online)
                {
                    store.Query(query, out header, out resultSet);
                    records = DataLoggerRecordUtils.GetDataLoggerRecordsFromQueryResult(resultSet,
                                                                                        header,
                                                                                        variablesToLogList,
                                                                                        insertOpCode,
                                                                                        insertVariableTimestamp);
                }
                else
                    records = new List<DataLoggerRecord>();
            }
            catch (Exception e)
            {
                throw new Exception("Failed to query the internal temporary table: " + e.Message);
            }

            return records;
        }

        private string GetQueryColumnsOrderedByVariableName()
        {
            var columnsOrderedByVariableName = string.Empty;
            foreach (var variable in variablesToLogList)
            {
                if (columnsOrderedByVariableName != string.Empty)
                    columnsOrderedByVariableName += ", ";

                columnsOrderedByVariableName += "\"" + variable.BrowseName + "\"";

                if (insertVariableTimestamp)
                    columnsOrderedByVariableName += ", \"" + variable.BrowseName + "_Timestamp\"";

                if (insertOpCode)
                    columnsOrderedByVariableName += ", \"" + variable.BrowseName + "_OpCode\"";
            }

            return columnsOrderedByVariableName;
        }

        private readonly Store store;
        private readonly string tableName;
        private readonly List<VariableToLog> variablesToLogList;
        private readonly bool insertOpCode;
        private readonly bool insertVariableTimestamp;
        private readonly bool preserveDataLoggerHistory;
    }

    public interface SupportStore
    {
        void InsertRecords(List<Record> records);
        void DeleteRecords(int numberOfRecordsToDelete);
        long RecordsCount();
        List<Record> QueryOlderEntries(int numberOfEntries);
    }

    public class PushAgentStoreDataLoggerWrapper : SupportStore
    {
        public PushAgentStoreDataLoggerWrapper(Store store,
                                               string tableName,
                                               List<VariableToLog> variablesToLogList,
                                               bool insertOpCode,
                                               bool insertVariableTimestamp)
        {
            this.store = store;
            this.tableName = tableName;
            this.variablesToLogList = variablesToLogList;
            this.insertOpCode = insertOpCode;
            this.insertVariableTimestamp = insertVariableTimestamp;

            try
            {
                CreateTable();
                table = GetTable();
                CreateColumns();
                CreateColumnIndex("Id", true);
                CreateColumnIndex("Timestamp", false);
                columns = GetTableColumnsOrderedByVariableName();
                idCount = GetMaxId();
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        public void DeleteRecords(int numberOfRecordsToDelete)
        {
            try
            {

                string query = $"DELETE FROM \"{tableName}\" " +
                               $"ORDER BY \"Timestamp\" ASC, \"Id\" ASC " +
                               $"LIMIT {numberOfRecordsToDelete}";

                store.Query(query, out _, out _);
            }
            catch (Exception e)
            {
                throw new Exception("Failed delete from PushAgent temporary table: " + e.Message);
            }
        }

        public void InsertRecords(List<Record> records)
        {
            List<DataLoggerRecord> dataLoggerRecords = records.Cast<DataLoggerRecord>().ToList();
            object[,] values = new object[records.Count, columns.Length];
            ulong tempIdCount = idCount;
            for (int i = 0; i < dataLoggerRecords.Count; ++i)
            {
                values[i, 0] = tempIdCount;
                values[i, 1] = records[i].timestamp;
                int j = 1;
                foreach (var variable in dataLoggerRecords.ElementAt(i).variables)
                {
                    values[i, ++j] = variable.value?.Value;
                    if (insertVariableTimestamp)
                        values[i, ++j] = variable.timestamp;
                    if (insertOpCode)
                        values[i, ++j] = variable.variableOpCode;
                }

                tempIdCount = GetNextInternalId(tempIdCount);
            }

            try
            {
                table.Insert(columns, values);
                idCount = tempIdCount;          // If all record are inserted then we update the idCount
            }
            catch (Exception e)
            {
                throw new Exception("Failed insert into PushAgent: " + tableName + " :" + e.Message);
            }
        }

        public List<Record> QueryOlderEntries(int numberOfEntries)
        {
            List<Record> records = null;
            object[,] resultSet;
            string[] header;

            try
            {
                string query = $"SELECT \"Timestamp\", {GetQueryColumnsOrderedByVariableName()} " +
                               $"FROM \"{tableName}\" " +
                               $"ORDER BY \"Timestamp\" ASC, \"Id\" ASC " +
                               $"LIMIT {numberOfEntries}";

                store.Query(query, out header, out resultSet);
                records = DataLoggerRecordUtils.GetDataLoggerRecordsFromQueryResult(resultSet,
                                                                                    header,
                                                                                    variablesToLogList,
                                                                                    insertOpCode,
                                                                                    insertVariableTimestamp).Cast<Record>().ToList();
            }
            catch (Exception e)
            {
                throw new Exception("Failed to query the internal PushAgent temporary table: " + e.Message);
            }

            return records;
        }

        public long RecordsCount()
        {
            object[,] resultSet;
            long result = 0;

            try
            {
                string query = $"SELECT COUNT(*) FROM \"{tableName}\"";
                store.Query(query, out _, out resultSet);
                result = ((long)resultSet[0, 0]);
            }
            catch (Exception e)
            {
                throw new Exception("Failed to query count: " + e.Message);
            }

            return result;
        }

        private UInt64 GetMaxId()
        {
            object[,] resultSet;

            try
            {
                string query = $"SELECT MAX(\"Id\") FROM \"{tableName}\"";
                store.Query(query, out _, out resultSet);

                if (resultSet[0, 0] != null)
                    return GetNextInternalId(UInt64.Parse(resultSet[0, 0].ToString()));
                else
                    return 0;
            }
            catch (Exception e)
            {
                Log.Error("PushAgent", "Failed to query maxid: " + e.Message);
                throw;
            }
        }

        private UInt64 GetNextInternalId(UInt64 currentId)
        {
            return currentId < Int64.MaxValue ? currentId + 1 : 0;
        }

        private void CreateTable()
        {
            try
            {
                store.AddTable(tableName);
            }
            catch (Exception e)
            {
                throw new Exception("Unable to create PushAgentTable: " + e.Message);
            }
        }

        private Table GetTable()
        {
            return store.Tables.FirstOrDefault(t => t.BrowseName == tableName);
        }

        private void CreateColumns()
        {
            try
            {
                table.AddColumn("Id", OpcUa.DataTypes.UInt64);
                table.AddColumn("Timestamp", OpcUa.DataTypes.DateTime);
                foreach (var variableToLog in variablesToLogList)
                {
                    table.AddColumn(variableToLog.BrowseName, variableToLog.ActualDataType);

                    if (insertVariableTimestamp)
                        table.AddColumn(variableToLog.BrowseName + "_Timestamp", OpcUa.DataTypes.DateTime);

                    if (insertOpCode)
                        table.AddColumn(variableToLog.BrowseName + "_OpCode", OpcUa.DataTypes.Int32);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Unable to create columns of internal PushAgentStore: " + e.Message);
            }
        }

        private void CreateColumnIndex(string columnName, bool unique)
        {
            string uniqueKeyWord = string.Empty;
            if (unique)
                uniqueKeyWord = "UNIQUE";
            try
            {
                string query = $"CREATE {uniqueKeyWord} INDEX \"{columnName}_index\" ON  \"{tableName}\"(\"{columnName}\")";
                store.Query(query, out _, out _);
            }
            catch (Exception e)
            {

            }
        }

        private string[] GetTableColumnsOrderedByVariableName()
        {
            List<string> columnNames = new List<string>();
            columnNames.Add("Id");
            columnNames.Add("Timestamp");
            foreach (var variableToLog in variablesToLogList)
            {
                columnNames.Add(variableToLog.BrowseName);

                if (insertVariableTimestamp)
                    columnNames.Add(variableToLog.BrowseName + "_Timestamp");

                if (insertOpCode)
                    columnNames.Add(variableToLog.BrowseName + "_OpCode");
            }

            return columnNames.ToArray();
        }

        private string GetQueryColumnsOrderedByVariableName()
        {
            string columnsOrderedByVariableName = string.Empty;
            foreach (var variable in variablesToLogList)
            {
                if (columnsOrderedByVariableName != string.Empty)
                    columnsOrderedByVariableName += ", ";

                columnsOrderedByVariableName += "\"" + variable.BrowseName + "\"";

                if (insertVariableTimestamp)
                    columnsOrderedByVariableName += ", \"" + variable.BrowseName + "_Timestamp\"";

                if (insertOpCode)
                    columnsOrderedByVariableName += ", \"" + variable.BrowseName + "_OpCode\"";
            }

            return columnsOrderedByVariableName;
        }

        private readonly Store store;
        private readonly Table table;
        private readonly string tableName;
        private readonly List<VariableToLog> variablesToLogList;
        private readonly string[] columns;
        private readonly bool insertOpCode;
        private readonly bool insertVariableTimestamp;
        private UInt64 idCount;
    }

    public class PushAgentStoreRowPerVariableWrapper : SupportStore
    {
        public PushAgentStoreRowPerVariableWrapper(SQLiteStore store, string tableName, bool insertOpCode)
        {
            this.store = store;
            this.tableName = tableName;
            this.insertOpCode = insertOpCode;

            try
            {
                CreateTable();
                table = GetTable();
                CreateColumns();
                CreateColumnIndex("Id", true);
                CreateColumnIndex("Timestamp", false);
                columns = GetTableColumnNames();
                idCount = GetMaxId();
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
        }

        public void DeleteRecords(int numberOfRecordsToDelete)
        {
            try
            {
                string query = $"DELETE FROM \"{tableName}\" " +
                               $"ORDER BY \"Timestamp\" ASC, \"Id\" ASC " +
                               $"LIMIT {numberOfRecordsToDelete}";

                store.Query(query, out _, out _);
            }
            catch (Exception e)
            {
                Log.Error("PushAgent", "Failed to delete from PushAgent temporary table " + e.Message);
                throw;
            }
        }

        public void InsertRecords(List<Record> records)
        {
            List<VariableRecord> variableRecords = records.Cast<VariableRecord>().ToList();
            object[,] values = new object[records.Count, columns.Length];
            UInt64 tempIdCount = idCount;
            for (int i = 0; i < variableRecords.Count; ++i)
            {
                values[i, 0] = tempIdCount;
                values[i, 1] = variableRecords[i].timestamp.Value;
                values[i, 2] = variableRecords[i].variableId;
                values[i, 3] = variableRecords[i].serializedValue;
                if (insertOpCode)
                    values[i, 4] = variableRecords[i].variableOpCode;

                tempIdCount = GetNextInternalId(tempIdCount);
            }

            try
            {
                table.Insert(columns, values);
                idCount = tempIdCount;
            }
            catch (Exception e)
            {
                Log.Error("PushAgent", "Failed insert into PushAgent: " + e.Message);
                throw;
            }
        }

        public List<Record> QueryOlderEntries(int numberOfEntries)
        {
            List<VariableRecord> records = new List<VariableRecord>();
            object[,] resultSet;
            string[] header;

            try
            {
                string query = $"SELECT {GetQueryColumns()} " +
                               $"FROM \"{tableName}\" " +
                               $"ORDER BY \"Timestamp\" ASC, \"Id\" ASC " +
                               $"LIMIT {numberOfEntries}";

                store.Query(query, out header, out resultSet);

                var rowCount = resultSet != null ? resultSet.GetLength(0) : 0;
                for (int i = 0; i < rowCount; ++i)
                {
                    int? opCodeValue = (int?)null;
                    if (insertOpCode)
                    {
                        if (resultSet[i, 3] == null)
                            opCodeValue = null;
                        else
                            opCodeValue = int.Parse(resultSet[i, 3].ToString());
                    }

                    VariableRecord record;
                    if (insertOpCode)
                        record = new VariableRecord(GetTimestamp(resultSet[i, 0]),
                                                    resultSet[i, 1].ToString(),
                                                    null,
                                                    resultSet[i, 2].ToString(),
                                                    opCodeValue);
                    else
                        record = new VariableRecord(GetTimestamp(resultSet[i, 0]),
                                                    resultSet[i, 1].ToString(),
                                                    null,
                                                    resultSet[i, 2].ToString());
                    records.Add(record);
                }
            }
            catch (Exception e)
            {
                Log.Error("PushAgent", "Failed to query older entries PushAgentStore: " + e.Message);
                throw;
            }

            return records.Cast<Record>().ToList();
        }

        public long RecordsCount()
        {
            object[,] resultSet;
            long result = 0;

            try
            {
                string query = $"SELECT COUNT(*) FROM \"{tableName}\"";

                store.Query(query, out _, out resultSet);
                result = ((long)resultSet[0, 0]);
            }
            catch (Exception e)
            {
                Log.Error("PushAgent", "Failed to query count: " + e.Message);
                throw;
            }

            return result;
        }

        private ulong GetMaxId()
        {
            object[,] resultSet;

            try
            {
                string query = $"SELECT MAX(\"ID\") FROM \"{tableName}\"";

                store.Query(query, out _, out resultSet);

                if (resultSet[0, 0] != null)
                    return GetNextInternalId(UInt64.Parse(resultSet[0, 0].ToString()));
                else
                    return 0;
            }
            catch (Exception e)
            {
                Log.Error("PushAgent", "Failed to query maxid: " + e.Message);
                throw;
            }
        }

        private UInt64 GetNextInternalId(UInt64 currentId)
        {
            return currentId < Int64.MaxValue ? currentId + 1 : 0;
        }

        private void CreateTable()
        {
            try
            {
                store.AddTable(tableName);
            }
            catch (Exception e)
            {
                throw new Exception("Unable to create PushAgentTable: " + e.Message);
            }
        }

        private Table GetTable()
        {
            return store.Tables.FirstOrDefault(t => t.BrowseName == tableName);
        }

        private void CreateColumns()
        {
            try
            {
                table.AddColumn("Id", OpcUa.DataTypes.UInt64);
                table.AddColumn("Timestamp", OpcUa.DataTypes.DateTime);
                table.AddColumn("VariableId", OpcUa.DataTypes.String);
                table.AddColumn("Value", OpcUa.DataTypes.String);

                if (insertOpCode)
                    table.AddColumn("OpCode", OpcUa.DataTypes.Int32);
            }
            catch (Exception e)
            {
                throw new Exception("Unable to create columns of internal PushAgentTable: " + e.Message);
            }
        }

        private void CreateColumnIndex(string columnName, bool unique)
        {
            string uniqueKeyWord = string.Empty;
            if (unique)
                uniqueKeyWord = "UNIQUE";
            try
            {
                string query = $"CREATE {uniqueKeyWord} INDEX \"{columnName}_index\" ON  \"{tableName}\"(\"{columnName}\")";
                store.Query(query, out _, out _);
            }
            catch (Exception e)
            {

            }
        }

        private string[] GetTableColumnNames()
        {
            if (table == null)
                return null;

            var result = new List<string>();
            foreach (var column in table.Columns)
                result.Add(column.BrowseName);

            return result.ToArray();
        }

        private string GetQueryColumns()
        {
            string columns = "\"Timestamp\", ";
            columns += "\"VariableId\", ";
            columns += "\"Value\"";

            if (insertOpCode)
                columns += ", OpCode";

            return columns;
        }

        private DateTime GetTimestamp(object value)
        {
            if (Type.GetTypeCode(value.GetType()) == TypeCode.DateTime)
                return ((DateTime)value);
            else
                return DateTime.SpecifyKind(DateTime.Parse(value.ToString()), DateTimeKind.Utc);
        }

        private readonly SQLiteStore store;
        private readonly string tableName;
        private readonly Table table;
        private readonly string[] columns;
        private readonly bool insertOpCode;
        private UInt64 idCount;
    }

    public class DataLoggerStatusStoreWrapper
    {
        public DataLoggerStatusStoreWrapper(Store store,
                                            string tableName,
                                            List<VariableToLog> variablesToLogList,
                                            bool insertOpCode,
                                            bool insertVariableTimestamp)
        {
            this.store = store;
            this.tableName = tableName;
            this.variablesToLogList = variablesToLogList;
            this.insertOpCode = insertOpCode;
            this.insertVariableTimestamp = insertVariableTimestamp;

            try
            {
                CreateTable();
                table = GetTable();
                CreateColumns();
                columns = GetTableColumnsOrderedByVariableName();
            }
            catch (Exception e)
            {
                throw new Exception("Unable to initialize internal DataLoggerStatusStoreWrapper " + e.Message);
            }
        }

        public void UpdateRecord(UInt64 rowId)
        {
            if (RecordsCount() == 0)
            {
                InsertRecord(rowId);
                return;
            }

            try
            {
                string query = $"UPDATE \"{tableName}\" SET \"RowId\" = {rowId} WHERE \"Id\"= 1";

                store.Query(query, out _, out _);
            }
            catch (Exception e)
            {
                throw new Exception("Failed to update internal DataLoggerStatusStore: " + e.Message);
            }
        }

        public void InsertRecord(UInt64 rowId)
        {
            var values = new object[1, columns.Length];

            values[0, 0] = 1;
            values[0, 1] = rowId;

            try
            {
                table.Insert(columns, values);
            }
            catch (Exception e)
            {
                throw new Exception("Failed to update internal DataLoggerStatusStore: " + e.Message);
            }
        }

        public UInt64? QueryStatus()
        {
            object[,] resultSet;
            string[] header;

            try
            {
                string query = $"SELECT \"RowId\" FROM \"{tableName}\"";

                store.Query(query, out header, out resultSet);

                if (resultSet[0, 0] != null)
                    return UInt64.Parse(resultSet[0, 0].ToString());

                return null;
            }
            catch (Exception e)
            {
                throw new Exception("Failed to query internal DataLoggerStatusStore: " + e.Message);
            }
        }

        public long RecordsCount()
        {
            object[,] resultSet;
            long result = 0;

            try
            {
                string query = $"SELECT COUNT(*) FROM \"{tableName}\"";

                store.Query(query, out _, out resultSet);
                result = ((long)resultSet[0, 0]);
            }
            catch (Exception e)
            {
                throw new Exception("Failed to query count: " + e.Message);
            }

            return result;
        }

        private void CreateTable()
        {
            try
            {
                store.AddTable(tableName);
            }
            catch (Exception e)
            {
                throw new Exception("Unable to create internal table to DataLoggerStatusStore: " + e.Message);
            }
        }

        private Table GetTable()
        {
            return store.Tables.FirstOrDefault(t => t.BrowseName == tableName);
        }

        private void CreateColumns()
        {
            try
            {
                table.AddColumn("Id", OpcUa.DataTypes.Int32);

                // We need to store only the last query's last row's id to retrieve the datalogger row
                table.AddColumn("RowId", OpcUa.DataTypes.Int64);
            }
            catch (Exception e)
            {
                throw new Exception("Unable to create columns of internal DataLoggerStatusStore: " + e.Message);
            }
        }

        private string[] GetTableColumnsOrderedByVariableName()
        {
            List<string> columnNames = new List<string>();
            columnNames.Add("Id");
            columnNames.Add("RowId");

            return columnNames.ToArray();
        }

        private readonly Store store;
        private readonly Table table;
        private readonly string tableName;
        private readonly List<VariableToLog> variablesToLogList;
        private readonly string[] columns;
        private readonly bool insertOpCode;
        private readonly bool insertVariableTimestamp;
    }

    public class DataLoggerRecordPuller
    {
        public DataLoggerRecordPuller(IUAObject logicObject,
                                      NodeId dataLoggerNodeId,
                                      SupportStore pushAgentStore,
                                      DataLoggerStatusStoreWrapper statusStoreWrapper,
                                      DataLoggerStoreWrapper dataLoggerStore,
                                      bool preserveDataLoggerHistory,
                                      bool pushByRow,
                                      int pullPeriod,
                                      int numberOfVariablesToLog)
        {
            this.logicObject = logicObject;
            this.pushAgentStore = pushAgentStore;
            this.statusStoreWrapper = statusStoreWrapper;
            this.dataLoggerStore = dataLoggerStore;
            this.dataLoggerNodeId = dataLoggerNodeId;
            this.preserveDataLoggerHistory = preserveDataLoggerHistory;
            this.pushByRow = pushByRow;
            this.numberOfVariablesToLog = numberOfVariablesToLog;

            if (this.preserveDataLoggerHistory)
            {
                UInt64? dataLoggerMaxId = this.dataLoggerStore.GetDataLoggerMaxId();

                if (statusStoreWrapper.RecordsCount() == 1)
                    lastPulledRecordId = statusStoreWrapper.QueryStatus();

                // Check if DataLogger has elements or if the maximum id is greater than lastPulledRecordId
                if (dataLoggerMaxId == null || (dataLoggerMaxId.HasValue && dataLoggerMaxId < lastPulledRecordId))
                    lastPulledRecordId = Int64.MaxValue;  // We have no elements in DataLogger so we will restart the count from 0
            }

            lastInsertedValues = new Dictionary<string, UAValue>();

            dataLoggerPullTask = new PeriodicTask(PullDataLoggerRecords, pullPeriod, this.logicObject);
            dataLoggerPullTask.Start();
        }

        public DataLoggerRecordPuller(IUAObject logicObject,
                                      NodeId dataLoggerNodeId,
                                      SupportStore pushAgentStore,
                                      DataLoggerStoreWrapper dataLoggerStore,
                                      bool preserveDataLoggerHistory,
                                      bool pushByRow,
                                      int pullPeriod,
                                      int numberOfVariablesToLog)
        {
            this.logicObject = logicObject;
            this.pushAgentStore = pushAgentStore;
            this.dataLoggerStore = dataLoggerStore;
            this.dataLoggerNodeId = dataLoggerNodeId;
            this.preserveDataLoggerHistory = preserveDataLoggerHistory;
            this.pushByRow = pushByRow;
            this.numberOfVariablesToLog = numberOfVariablesToLog;

            lastInsertedValues = new Dictionary<string, UAValue>();

            dataLoggerPullTask = new PeriodicTask(PullDataLoggerRecords, pullPeriod, this.logicObject);
            dataLoggerPullTask.Start();
        }

        public void StopPullTask()
        {
            dataLoggerPullTask.Cancel();
        }

        private void PullDataLoggerRecords()
        {
            try
            {
                dataLoggerPulledRecords = null;
                if (!preserveDataLoggerHistory || lastPulledRecordId == null)
                    dataLoggerPulledRecords = dataLoggerStore.QueryNewEntries();
                else
                    dataLoggerPulledRecords = dataLoggerStore.QueryNewEntriesUsingLastQueryId(lastPulledRecordId.Value);

                if (dataLoggerPulledRecords.Count > 0)
                {
                    InsertDataLoggerRecordsIntoPushAgentStore();

                    if (!preserveDataLoggerHistory)
                        dataLoggerStore.DeletePulledRecords();
                    else
                    {
                        lastPulledRecordId = dataLoggerStore.GetMaxIdFromTemporaryTable();

                        statusStoreWrapper.UpdateRecord(lastPulledRecordId.Value);
                    }

                    dataLoggerPulledRecords.Clear();
                }
            }
            catch (Exception e)
            {
                if (dataLoggerStore.GetStoreStatus() != StoreStatus.Offline)
                {
                    Log.Error("PushAgent", "Unable to retrieve data from DataLogger store: " + e.Message);
                    StopPullTask();
                }
            }
        }

        private void InsertDataLoggerRecordsIntoPushAgentStore()
        {
            if (!IsStoreSpaceAvailable())
                return;

            if (pushByRow)
                InsertRowsIntoPushAgentStore();
            else
                InsertVariableRecordsIntoPushAgentStore();
        }

        private VariableRecord CreateVariableRecord(VariableRecord variable, DateTime recordTimestamp)
        {
            VariableRecord variableRecord;
            if (variable.timestamp == null)
                variableRecord = new VariableRecord(recordTimestamp,
                                                    variable.variableId,
                                                    variable.value,
                                                    variable.serializedValue,
                                                    variable.variableOpCode);
            else
                variableRecord = new VariableRecord(variable.timestamp,
                                                    variable.variableId,
                                                    variable.value,
                                                    variable.serializedValue,
                                                    variable.variableOpCode);



            return variableRecord;
        }

        private void InsertRowsIntoPushAgentStore()
        {
            int numberOfStorableRecords = CalculateNumberOfElementsToInsert();

            if (dataLoggerPulledRecords.Count > 0)
                pushAgentStore.InsertRecords(dataLoggerPulledRecords.Cast<Record>().ToList().GetRange(0, numberOfStorableRecords));
        }

        private void InsertVariableRecordsIntoPushAgentStore()
        {
            int numberOfStorableRecords = CalculateNumberOfElementsToInsert();

            // Temporary dictionary is used to update values, once the records are inserted then the content is copied to lastInsertedValues
            Dictionary<string, UAValue> tempLastInsertedValues = lastInsertedValues.Keys.ToDictionary(_ => _, _ => lastInsertedValues[_]);
            List<VariableRecord> pushAgentRecords = new List<VariableRecord>();
            foreach (var record in dataLoggerPulledRecords.GetRange(0, numberOfStorableRecords))
            {
                foreach (var variable in record.variables)
                {
                    VariableRecord variableRecord = CreateVariableRecord(variable, record.timestamp.Value);
                    if (GetSamplingMode() == SamplingMode.VariableChange)
                    {
                        if (!tempLastInsertedValues.ContainsKey(variable.variableId))
                        {
                            if (variableRecord.serializedValue != null)
                            {
                                pushAgentRecords.Add(variableRecord);
                                tempLastInsertedValues.Add(variableRecord.variableId, variableRecord.value);
                            }
                        }
                        else
                        {
                            if (variable.value != tempLastInsertedValues[variable.variableId] && variableRecord.serializedValue != null)
                            {
                                pushAgentRecords.Add(variableRecord);
                                tempLastInsertedValues[variableRecord.variableId] = variableRecord.value;
                            }
                        }
                    }
                    else
                    {
                        if (variableRecord.serializedValue != null)
                            pushAgentRecords.Add(variableRecord);
                    }
                }
            }

            if (pushAgentRecords.Count > 0)
            {
                pushAgentStore.InsertRecords(pushAgentRecords.Cast<Record>().ToList());

                if (GetSamplingMode() == SamplingMode.VariableChange)
                    lastInsertedValues = tempLastInsertedValues.Keys.ToDictionary(_ => _, _ => tempLastInsertedValues[_]);
            }
        }

        private int GetMaximumStoreCapacity()
        {
            return logicObject.GetVariable("MaximumStoreCapacity").Value;
        }

        private SamplingMode GetSamplingMode()
        {
            var dataLogger = InformationModel.Get<DataLogger>(dataLoggerNodeId);
            return dataLogger.SamplingMode;
        }

        private int CalculateNumberOfElementsToInsert()
        {
            // Calculate the number of records that can be effectively stored
            int numberOfStorableRecords;

            if (pushByRow)
                numberOfStorableRecords = (GetMaximumStoreCapacity() - (int)pushAgentStore.RecordsCount());
            else
            {
                if (GetSamplingMode() == SamplingMode.VariableChange)
                    numberOfStorableRecords = (GetMaximumStoreCapacity() - (int)pushAgentStore.RecordsCount());
                else
                    numberOfStorableRecords = (int)Math.Floor((double)(GetMaximumStoreCapacity() - (int)pushAgentStore.RecordsCount()) / numberOfVariablesToLog);
            }

            if (numberOfStorableRecords > dataLoggerPulledRecords.Count)
                numberOfStorableRecords = dataLoggerPulledRecords.Count;

            return numberOfStorableRecords;
        }

        private bool IsStoreSpaceAvailable()
        {
            if (pushAgentStore.RecordsCount() >= GetMaximumStoreCapacity() - 1)
            {
                Log.Warning("PushAgent", "Maximum store capacity reached! Skipping...");
                return false;
            }

            var percentageStoreCapacity = ((double)pushAgentStore.RecordsCount() / GetMaximumStoreCapacity()) * 100;
            if (percentageStoreCapacity >= 70)
                Log.Warning("PushAgent", "Store capacity 70% reached!");

            return true;
        }

        private List<DataLoggerRecord> dataLoggerPulledRecords;
        private UInt64? lastPulledRecordId;
        private readonly PeriodicTask dataLoggerPullTask;
        private readonly SupportStore pushAgentStore;
        private readonly DataLoggerStatusStoreWrapper statusStoreWrapper;
        private readonly DataLoggerStoreWrapper dataLoggerStore;
        private readonly bool preserveDataLoggerHistory;
        private readonly bool pushByRow;
        private readonly IUAObject logicObject;
        private readonly int numberOfVariablesToLog;
        private readonly NodeId dataLoggerNodeId;
        private Dictionary<string, UAValue> lastInsertedValues;
    }

    public class MQTTConnector
    {
        public MQTTConnector(IUAObject context,
                             string brokerIpAddressVariable,
                             string clientID,
                             MqttMsgPublishedEventHandler PublishClientMqttMsgPublished = null,
                             int port = 1883)
        {
            this.context = context;
            this.clientID = clientID;
            this.onPublishedCallback = PublishClientMqttMsgPublished;

            mqttClient = new MqttClient(brokerIpAddressVariable, port, false, null, null, MqttSslProtocols.None);
            mqttClient.MqttMsgPublished += onPublishedCallback;
            mqttClient.ConnectionClosed += ConnectionClosedHandler;

            onTryConnectEvent = new AutoResetEvent(false);
            connectManualResetEvent = new ManualResetEventSlim(false);

            onTryConnectTask = new LongRunningTask(OnTryConnect, context);
            onTryConnectTask.Start();

            onTryConnectEvent.Set();
        }

        public MQTTConnector(IUAObject context,
                             string brokerIpAddressVariable,
                             string clientID,
                             string pathClientCert,
                             string passwordClientCert,
                             string pathCACert,
                             MqttMsgPublishedEventHandler PublishClientMqttMsgPublished = null,
                             int port = 8883)
        {
            if (port != MqttSettings.MQTT_BROKER_DEFAULT_SSL_PORT)
                Log.Warning("MQTTCloudConnector", "Selected a different port of SSL. Default Port is 8883");

            var clientCert = new X509Certificate2(pathClientCert, passwordClientCert);
            var caCert = new X509Certificate2(pathCACert, "");

            this.context = context;
            this.clientID = clientID;
            this.onPublishedCallback = PublishClientMqttMsgPublished;

            mqttClient = new MqttClient(brokerIpAddressVariable,
                                        port,
                                        true,
                                        caCert,
                                        clientCert,
                                        MqttSslProtocols.TLSv1_2,
                                        RemoteCertificateValidationCallback);

            mqttClient.MqttMsgPublished += this.onPublishedCallback;
            mqttClient.ConnectionClosed += ConnectionClosedHandler;

            onTryConnectEvent = new AutoResetEvent(false);
            connectManualResetEvent = new ManualResetEventSlim(false);

            onTryConnectTask = new LongRunningTask(OnTryConnect, context);
            onTryConnectTask.Start();

            onTryConnectEvent.Set();
        }

        public MQTTConnector(IUAObject context,
                             string brokerIpAddressVariable,
                             string clientID,
                             string username,
                             string password,
                             bool useIoTHub,
                             MqttMsgPublishedEventHandler PublishClientMqttMsgPublished = null,
                             int port = 1883)
        {
            this.context = context;
            this.clientID = clientID;
            this.username = username;
            this.password = password;
            this.onPublishedCallback = PublishClientMqttMsgPublished;
            useUsernamePassword = true;

            if (useIoTHub)
                mqttClient = new MqttClient(brokerIpAddressVariable,
                                            uPLibrary.Networking.M2Mqtt.MqttSettings.MQTT_BROKER_DEFAULT_SSL_PORT,
                                            true,
                                            null,
                                            null,
                                            MqttSslProtocols.TLSv1_2,
                                            null);
            else
                mqttClient = new MqttClient(brokerIpAddressVariable,
                                            port,
                                            false,
                                            null,
                                            null,
                                            MqttSslProtocols.None,
                                            null);

            mqttClient.MqttMsgPublished += onPublishedCallback;
            mqttClient.ConnectionClosed += ConnectionClosedHandler;

            onTryConnectEvent = new AutoResetEvent(false);
            connectManualResetEvent = new ManualResetEventSlim(false);

            onTryConnectTask = new LongRunningTask(OnTryConnect, context);
            onTryConnectTask.Start();

            onTryConnectEvent.Set();
        }

        public void Disconnect()
        {
            closing = true;
            onTryConnectEvent.Set();
            connectManualResetEvent.Set();
            connectTask?.Dispose();
            onTryConnectTask?.Dispose();

            try
            {
                if (mqttClient.IsConnected)
                    mqttClient.Disconnect();
            }
            catch (Exception)
            {

            }

            mqttClient.MqttMsgPublished -= onPublishedCallback;
            mqttClient.ConnectionClosed -= ConnectionClosedHandler;
        }

        public bool Connected()
        {
            return mqttClient.IsConnected;
        }

        private bool RemoteCertificateValidationCallback(object sender,
                                                         X509Certificate certificate,
                                                         X509Chain chain,
                                                         SslPolicyErrors sslPolicyErrors)
        {
            // This method is necessary
            return true;
        }

        private void OnTryConnect()
        {
            while (true)
            {
                onTryConnectEvent.WaitOne();
                if (closing)
                    return;

                connectTask = new DelayedTask(Connect, CalculateNextRetryTimeout(), context);
                connectTask.Start();
            }
        }

        public void Connect()
        {
            closing = false;
            try
            {
                if (!useUsernamePassword)
                    mqttClient.Connect(clientID, "", "", false, keepAlivePeriod);
                else
                    mqttClient.Connect(clientID, username, password, false, keepAlivePeriod);

                connectManualResetEvent.Set();
                numberOfRetries = 0;
            }
            catch (uPLibrary.Networking.M2Mqtt.Exceptions.MqttConnectionException e)
            {
                if (numberOfRetries == 0)
                    Log.Warning("PushAgent", "Connection Failed to broker " + e.Message);
                numberOfRetries++;
                onTryConnectEvent.Set();
            }
        }

        private int CalculateNextRetryTimeout()
        {
            var retryTimeoutMs = Math.Pow(2, numberOfRetries) * initialRetryTimeoutMs;
            retryTimeoutMs = Math.Min(retryTimeoutMs, maximumRetryTimeoutMs);

            return (int)retryTimeoutMs;
        }

        private void ConnectionClosedHandler(object sender, EventArgs e)
        {
            connectManualResetEvent.Reset();
            onTryConnectEvent.Set();
        }

        public void Publish(string records, string topic, bool retain, int qosLevel)
        {
            if (!mqttClient.IsConnected)
            {
                connectManualResetEvent.Wait();
                if (closing)
                    return;
            }

            mqttClient.Publish(topic,
                               System.Text.Encoding.UTF8.GetBytes(records), // message body
                               GetQoSLevel(qosLevel),
                               retain);
        }

        public void AddSubscriber(string topic,
                                  int qosLevel,
                                  MqttMsgPublishEventHandler subscribeClientMqttMsgPublishReceived)
        {
            mqttClient.MqttMsgPublishReceived += subscribeClientMqttMsgPublishReceived;

            mqttClient.Subscribe(new string[] { topic },
                new byte[] { GetQoSLevel(qosLevel) });
        }

        public void RemoveSubscriber(string topic)
        {
            mqttClient.Unsubscribe(new string[] { topic });
        }

        private byte GetQoSLevel(int qosLevel)
        {
            switch (qosLevel)
            {
                case 0:
                    return MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE;
                case 1:
                    return MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE;
                case 2:
                    return MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE;
                default:
                    return MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE;
            }
        }

        public readonly MqttClient mqttClient;
        private readonly MqttMsgPublishedEventHandler onPublishedCallback;
        public  string clientID;
        private readonly IUAObject context;
        private DelayedTask connectTask;
        public LongRunningTask onTryConnectTask;
        private AutoResetEvent onTryConnectEvent;
        private ManualResetEventSlim connectManualResetEvent;
        private bool closing = false;
        private int numberOfRetries = 0;
        private int initialRetryTimeoutMs = 1000;
        private int maximumRetryTimeoutMs = 60000;
        private readonly ushort keepAlivePeriod = 5; //seconds
        private readonly bool useUsernamePassword;
        private readonly string username;
        private readonly string password;
    }

    public class JSONBuilder
    {
        public JSONBuilder(bool insertOpCode, bool insertVariableTimestamp)
        {
            this.insertOpCode = insertOpCode;
            this.insertVariableTimestamp = insertVariableTimestamp;
        }

        public string CreateDataLoggerRowPacketFormatJSON(DataLoggerRowPacket packet)
        {
            var sb = new StringBuilder();
            var sw = new StringWriter(sb);
            using (var writer = new JsonTextWriter(sw))
            {

                writer.Formatting = Formatting.None;

                writer.WriteStartObject();
                writer.WritePropertyName("Timestamp");
                writer.WriteValue(packet.timestamp);
                writer.WritePropertyName("ClientId");
                writer.WriteValue((string)Project.Current.GetVariable("Model/RetentiveMachineData/MachineName").Value);
                writer.WritePropertyName("Rows");
                writer.WriteStartArray();
                foreach (var record in packet.records)
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("RowTimestamp");
                    //writer.WriteValue(record.timestamp);      //////TOM
                    var offset = TimeZone.CurrentTimeZone.GetUtcOffset(record.timestamp.Value);
                    writer.WriteValue(((DateTime)record.timestamp).AddHours(offset.Hours));       //////TOM
                    writer.WritePropertyName("Variables");
                    writer.WriteStartArray();
                    foreach (var variable in record.variables)
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName("VariableName");
                        writer.WriteValue(variable.variableId);
                        writer.WritePropertyName("Value");
                        writer.WriteValue(variable.value?.Value);

                        if (insertVariableTimestamp)
                        {
                            writer.WritePropertyName("VariableTimestamp");
                            writer.WriteValue(((DateTime)record.timestamp).AddHours(offset.Hours));       //////TOM
                        }

                        if (insertOpCode)
                        {
                            writer.WritePropertyName("VariableOpCode");
                            writer.WriteValue(variable.variableOpCode);
                        }

                        writer.WriteEndObject();
                    }
                    writer.WriteEnd();
                    writer.WriteEndObject();
                }
                writer.WriteEnd();
                writer.WriteEndObject();
            }

            return sb.ToString();
        }

        public string CreateVariablePacketFormatJSON(VariablePacket packet)
        {
            var sb = new StringBuilder();
            var sw = new StringWriter(sb);
            using (var writer = new JsonTextWriter(sw))
            {
                writer.Formatting = Formatting.None;

                writer.WriteStartObject();
                writer.WritePropertyName("Timestamp");
                writer.WriteValue(packet.timestamp);
                writer.WritePropertyName("ClientId");
                writer.WriteValue((string)Project.Current.GetVariable("Model/RetentiveMachineData/MachineName").Value);
                writer.WritePropertyName("Records");
                writer.WriteStartArray();
                foreach (var record in packet.records)
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("VariableName");
                    writer.WriteValue(record.variableId);
                    writer.WritePropertyName("SerializedValue");
                    writer.WriteValue(record.serializedValue);
                    writer.WritePropertyName("VariableTimestamp");
                    writer.WriteValue(record.timestamp);

                    if (insertOpCode)
                    {
                        writer.WritePropertyName("VariableOpCode");
                        writer.WriteValue(record.variableOpCode);
                    }

                    writer.WriteEndObject();
                }
                writer.WriteEnd();
                writer.WriteEndObject();
            }

            return sb.ToString();
        }

        private readonly bool insertOpCode;
        private readonly bool insertVariableTimestamp;
    }
}

public class PushAgent : BaseNetLogic
{
    private PeriodicTask periodicTask;
    private PeriodicTask pTCheckConnection;

    IUAVariable temperature = Project.Current.GetVariable("Model/ProcessData/Temperature");
    IUAVariable timeToFinish = Project.Current.GetVariable("Model/ProcessData/TimeToFinish");
    IUAVariable machineName = Project.Current.GetVariable("Model/RetentiveMachineData/MachineName");

    public override void Start()
    {
        CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("en-US");
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        LogicObject.GetVariable("ClientId").Value = new string(Enumerable.Repeat(chars, 20)
            .Select(s => s[random.Next(s.Length)]).ToArray());
        LoadPushAgentConfiguration();
        ConfigureStores();
        ConfigureDataLoggerRecordPuller();
        ////ConfigureMQTT();

        //onPublishedEvent = new AutoResetEvent(false);
        //onAfterPublishTask = new LongRunningTask(OnAfterMessagePublished, LogicObject);
        //onAfterPublishTask.Start();


        //try
        //{
        //    LoadPushAgentConfiguration();
        //    CheckMQTTParameters();

        //    ConfigureStores();
        //    ConfigureDataLoggerRecordPuller();
        //    ConfigureMQTT();

        //    onPublishedEvent = new AutoResetEvent(false);
        //    onAfterPublishTask = new LongRunningTask(OnAfterMessagePublished, LogicObject);
        //    onAfterPublishTask.Start();

        //    StartFetchTimer();
        //    initialized = true;
        //    //periodicTask = new PeriodicTask(Temperature_VariableChange, 800, LogicObject);
        //    //periodicTask.Start();
        //    var temperature = Project.Current.GetVariable("Model/ProcessData/Temperature");
        //    var timeToFinish = Project.Current.GetVariable("Model/ProcessData/TimeToFinish");
        //    temperature.VariableChange += TimeToFinish_VariableChange;
        //    timeToFinish.VariableChange += TimeToFinish_VariableChange;

        //}
        //catch (Exception e)
        //{
        //    Log.Warning("PushAgent", "Unable to initialize, an error occurred: " + e.Message);
        //}
    }

    private void MachineName_VariableChange(object sender, VariableChangeEventArgs e)
    {
        //AttachNewMachine((string)machineName.Value);
    }

    private void CheckMqttConn()
    {
        LogicObject.GetVariable("Connected").Value = MqttConnectorInstance.mqttconn.Connected();
    }

    private void TimeToFinish_VariableChange(object sender, VariableChangeEventArgs e)
    {
        string message = @"{ ""MachineName"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineName").Value +
                         @""",""Temperature"":""" + Project.Current.GetVariable("Model/ProcessData/Temperature").Value +
                         @""",""Running"":""" + Project.Current.GetVariable("Model/ProcessData/Running").Value +
                         @""",""RecipeName"":""" + Project.Current.GetVariable("Model/Job/Recipe").Value +
                         @""",""LPGConsumption"":""" + Project.Current.GetVariable("Model/ProcessData/LPGConsumption").Value +
                         @""",""ElectricalConsumption"":""" + Project.Current.GetVariable("Model/ProcessData/ElectricalConsumption").Value +
                         @""",""FanIn"":""" + Project.Current.GetVariable("Model/ProcessData/FanIn").Value +
                         @""",""FanOut"":""" + Project.Current.GetVariable("Model/ProcessData/FanOut").Value +
                         @""",""SetPoint"":""" + Project.Current.GetVariable("Model/ProcessData/SetPoint").Value +
                         @""",""Phase"":""" + Project.Current.GetVariable("Model/ProcessData/Phase").Value +
                         @""",""FilterLimit"":""" + Project.Current.GetVariable("Model/ProcessData/FilterLimit").Value +
                         @""",""FilterUse"":""" + Project.Current.GetVariable("Model/ProcessData/FilterUse").Value +
                         @""",""Cycle"":""" + Project.Current.GetVariable("Model/ProcessData/Cycle").Value +
                         @""",""TimeToFinish"":""" + Project.Current.GetVariable("Model/ProcessData/TimeToFinish").Value +
                         @"""}";
        mqttClientConnector.Publish(message, "/iotdemo-hotdata", false, 2);
    }

    private void Temperature_VariableChange()
    {
        string message = @"{ ""MachineName"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineName").Value +
                         @""",""Temperature"":""" + Project.Current.GetVariable("Model/ProcessData/Temperature").Value +
                         @""",""Running"":""" + Project.Current.GetVariable("Model/ProcessData/Running").Value +
                         @""",""RecipeName"":""" + Project.Current.GetVariable("Model/Job/Recipe").Value +
                         @""",""LPGConsumption"":""" + Project.Current.GetVariable("Model/ProcessData/LPGConsumption").Value +
                         @""",""ElectricalConsumption"":""" + Project.Current.GetVariable("Model/ProcessData/ElectricalConsumption").Value +
                         @""",""FanIn"":""" + Project.Current.GetVariable("Model/ProcessData/FanIn").Value +
                         @""",""FanOut"":""" + Project.Current.GetVariable("Model/ProcessData/FanOut").Value +
                         @""",""SetPoint"":""" + Project.Current.GetVariable("Model/ProcessData/SetPoint").Value +
                         @""",""Phase"":""" + Project.Current.GetVariable("Model/ProcessData/Phase").Value +
                         @""",""FilterLimit"":""" + Project.Current.GetVariable("Model/ProcessData/FilterLimit").Value +
                         @""",""FilterUse"":""" + Project.Current.GetVariable("Model/ProcessData/FilterUse").Value +
                         @""",""Cycle"":""" + Project.Current.GetVariable("Model/ProcessData/Cycle").Value +
                         @""",""TimeToFinish"":""" + Project.Current.GetVariable("Model/ProcessData/TimeToFinish").Value +
                         @"""}";
        mqttClientConnector.Publish(message, "/iotdemo-hotdata", false, 2);
    }

    public override void Stop()
    {
        closing = true;

        if (initialized)
        {
            onPublishedEvent.Set();
            if (mqttClientConnector != null)
            {
                mqttClientConnector.Disconnect();
            }
                
        }

        var temperature = Project.Current.GetVariable("Model/ProcessData/Temperature");
        var timeToFinish = Project.Current.GetVariable("Model/ProcessData/TimeToFinish");
        temperature.VariableChange -= TimeToFinish_VariableChange;
        timeToFinish.VariableChange -= TimeToFinish_VariableChange;
        if (pTCheckConnection != null) {
            pTCheckConnection.Dispose();
        }
        LogicObject.GetVariable("Connected").Value = false;
    }

    private void ConfigureMQTT()
    {
        var username = pushAgentConfigurationParameters.mqtttConfigurationParameters.username;
        var password = pushAgentConfigurationParameters.mqtttConfigurationParameters.password;
        if ((useIoTHub && !string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password)) ||
            (!useIoTHub && !string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password)))
        {
            // IoTHub or classic username and password authentication
            mqttClientConnector = new MQTTConnector(LogicObject,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.brokerIPAddress,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.clientId,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.username,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.password,
                                                    useIoTHub,
                                                    OnMessagePublished,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.brokerPort);
        }
        else if (pushAgentConfigurationParameters.mqtttConfigurationParameters.useSSL)
        {
            // SSL authentication
            mqttClientConnector = new MQTTConnector(LogicObject,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.brokerIPAddress,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.clientId,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.pathClientCert,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.passwordClientCert,
                                                    pushAgentConfigurationParameters.mqtttConfigurationParameters.pathCACert,
                                                    OnMessagePublished);
        }
        else
        {
            // Anonymous authentication
            mqttClientConnector = new MQTTConnector(LogicObject,
                                        pushAgentConfigurationParameters.mqtttConfigurationParameters.brokerIPAddress,
                                        pushAgentConfigurationParameters.mqtttConfigurationParameters.clientId,
                                        OnMessagePublished);
        }

        MqttConnectorInstance.mqttconn = mqttClientConnector;
    }

    private void ConfigureDataLoggerRecordPuller()
    {
        int period = 10000;
        if (pushAgentConfigurationParameters.preserveDataLoggerHistory)
        {
            dataLoggerRecordPuller = new DataLoggerRecordPuller(LogicObject,
                                                                LogicObject.GetVariable("DataLogger").Value,
                                                                pushAgentStore,
                                                                statusStoreWrapper,
                                                                dataLoggerStore,
                                                                pushAgentConfigurationParameters.preserveDataLoggerHistory,
                                                                pushAgentConfigurationParameters.pushFullSample,
                                                                period,
                                                                pushAgentConfigurationParameters.datalogger.VariablesToLog.ToList().Count);
        }
        else
        {
            dataLoggerRecordPuller = new DataLoggerRecordPuller(LogicObject,
                                                                LogicObject.GetVariable("DataLogger").Value,
                                                                pushAgentStore,
                                                                dataLoggerStore,
                                                                pushAgentConfigurationParameters.preserveDataLoggerHistory,
                                                                pushAgentConfigurationParameters.pushFullSample,
                                                                period,
                                                                pushAgentConfigurationParameters.datalogger.VariablesToLog.ToList().Count);
        }
    }

    private void ConfigureStores()
    {
        string pushAgentStoreBrowseName = "PushAgentStore";
        string pushAgentFilename = "push_agent_store";
        CreatePushAgentStore(pushAgentStoreBrowseName, pushAgentFilename);

        var variableLogOpCode = pushAgentConfigurationParameters.datalogger.GetVariable("LogVariableOperationCode");
        insertOpCode = variableLogOpCode != null ? (bool)variableLogOpCode.Value : false;

        var variableTimestamp = pushAgentConfigurationParameters.datalogger.GetVariable("LogVariableTimestamp");
        insertVariableTimestamp = variableTimestamp != null ? (bool)variableTimestamp.Value : false;

        jsonCreator = new JSONBuilder(insertOpCode, insertVariableTimestamp);

        dataLoggerStore = new DataLoggerStoreWrapper(InformationModel.Get<FTOptix.Store.Store>(pushAgentConfigurationParameters.datalogger.Store),
                                            GetDataLoggerTableName(),
                                            pushAgentConfigurationParameters.datalogger.VariablesToLog.ToList(),
                                            insertOpCode,
                                            insertVariableTimestamp,
                                            pushAgentConfigurationParameters.preserveDataLoggerHistory);

        if (!pushAgentConfigurationParameters.pushFullSample)
        {
            string tableName = "PushAgentTableRowPerVariable";
            pushAgentStore = new PushAgentStoreRowPerVariableWrapper(LogicObject.Get<SQLiteStore>(pushAgentStoreBrowseName),
                                                                     tableName,
                                                                     insertOpCode);
        }
        else
        {
            string tableName = "PushAgentTableDataLogger";
            pushAgentStore = new PushAgentStoreDataLoggerWrapper(LogicObject.Get<SQLiteStore>(pushAgentStoreBrowseName),
                                                                tableName,
                                                                pushAgentConfigurationParameters.datalogger.VariablesToLog.ToList(),
                                                                insertOpCode,
                                                                insertVariableTimestamp);
            if (GetMaximumRecordsPerPacket() != 1)
            {
                Log.Warning("PushAgent", "For PushByRow mode maximum one row per packet is supported. Setting value to 1.");
                LogicObject.GetVariable("MaximumItemsPerPacket").Value = 1;
            }
        }

        if (pushAgentConfigurationParameters.preserveDataLoggerHistory)
        {
            string tableName = "DataLoggerStatusStore";
            statusStoreWrapper = new DataLoggerStatusStoreWrapper(LogicObject.Get<SQLiteStore>(pushAgentStoreBrowseName),
                                                                                            tableName,
                                                                                            pushAgentConfigurationParameters.datalogger.VariablesToLog.ToList(),
                                                                                            insertOpCode,
                                                                                            insertVariableTimestamp);
        }
    }

    private void OnMessagePublished(object sender, MqttMsgPublishedEventArgs e)
    {
        if (e.IsPublished)
            onPublishedEvent.Set();
    }

    private void OnAfterMessagePublished()
    {
        while (true)
        {
            try
            {
                if (onAfterPublishTask.IsCancellationRequested)
                    return;

                onPublishedEvent.WaitOne();
                if (closing)
                    return;

                if (pendingSendPacket != null)
                {
                    if (pushAgentConfigurationParameters.pushFullSample)
                        pushAgentStore.DeleteRecords(((DataLoggerRowPacket)pendingSendPacket).records.Count);
                    else
                        pushAgentStore.DeleteRecords(((VariablePacket)pendingSendPacket).records.Count);

                    pendingSendPacket = null;
                    StartFetchTimer();
                }
            }
            catch (Exception e)
            {
                OnFetchError(e.Message);
            }
        }
    }

    private void StartFetchTimer()
    {
        try
        {
            // Set the correct timeout by checking number of records to be sent
            if (pushAgentStore.RecordsCount() >= GetMaximumRecordsPerPacket())
                nextRestartTimeout = GetMinimumPublishTime();
            else
                nextRestartTimeout = GetMaximumPublishTime();

            restartDataFetchTask = new DelayedTask(OnRestartDataFetchTimer, 0, LogicObject);
            restartDataFetchTask.Start();
            restartDataFetchTaskRunning = true;
        }
        catch (Exception e)
        {
            OnFetchError(e.Message);
        }
    }

    private void OnRestartDataFetchTimer()
    {
        restartDataFetchTaskRunning = false;

        dataFetchTask = new DelayedTask(OnFetchRequired, nextRestartTimeout, LogicObject);
        dataFetchTask.Start();
        dataFetchTaskRunning = true;
    }

    private void OnFetchRequired()
    {
        dataFetchTaskRunning = false;

        if (pushAgentStore.RecordsCount() > 0)
            FetchData();
        else
            StartFetchTimer();
    }

    private void FetchData()
    {
        List<Record> records = null;

        try
        {
            records = pushAgentStore.QueryOlderEntries(GetMaximumRecordsPerPacket()).Cast<Record>().ToList();
        }
        catch (Exception e)
        {
            OnFetchError(e.Message);
        }

        if (records.Count > 0)
        {
            if (pushAgentConfigurationParameters.pushFullSample)
                pendingSendPacket = new DataLoggerRowPacket(DateTime.Now,
                                                            pushAgentConfigurationParameters.mqtttConfigurationParameters.clientId,
                                                            records.Cast<DataLoggerRecord>().ToList());
            else
                pendingSendPacket = new VariablePacket(DateTime.Now,
                                                       pushAgentConfigurationParameters.mqtttConfigurationParameters.clientId,
                                                       records.Cast<VariableRecord>().ToList());

            string json = GenerateJSON(pendingSendPacket);
            mqttClientConnector.Publish(json,
                                        pushAgentConfigurationParameters.mqtttConfigurationParameters.brokerTopic,
                                        false,
                                        pushAgentConfigurationParameters.mqtttConfigurationParameters.qos);
        }
    }

    private string GenerateJSON(Packet packet)
    {
        if (pushAgentConfigurationParameters.pushFullSample)
            return jsonCreator.CreateDataLoggerRowPacketFormatJSON((DataLoggerRowPacket)packet);
        else
            return jsonCreator.CreateVariablePacketFormatJSON((VariablePacket)packet);
    }

    private void OnFetchError(string message)
    {
        Log.Error("PushAgent", "Error while pushing data: " + message);
        dataLoggerRecordPuller.StopPullTask();
        onAfterPublishTask.Cancel();

        if (restartDataFetchTaskRunning)
            restartDataFetchTask.Cancel();

        if (dataFetchTaskRunning)
            dataFetchTask.Cancel();
    }

    private void LoadMQTTConfiguration()
    {
        pushAgentConfigurationParameters.mqtttConfigurationParameters = new MQTTConfigurationParameters
        {
            clientId = LogicObject.GetVariable("ClientId").Value,
            brokerIPAddress = LogicObject.GetVariable("BrokerIPAddress").Value,
            brokerPort = LogicObject.GetVariable("BrokerPort").Value,
            brokerTopic = "/" + LogicObject.GetVariable("BrokerTopic").Value,
            qos = LogicObject.GetVariable("QoS").Value,
            useSSL = LogicObject.GetVariable("UseSSL").Value,
            pathCACert = ResourceUriValueToAbsoluteFilePath(LogicObject.GetVariable("UseSSL/CACert").Value),
            pathClientCert = ResourceUriValueToAbsoluteFilePath(LogicObject.GetVariable("UseSSL/ClientCert").Value),
            passwordClientCert = LogicObject.GetVariable("UseSSL/ClientCertPassword").Value,
            username = LogicObject.GetVariable("Username").Value,
            password = LogicObject.GetVariable("Password").Value
        };
    }

    private void LoadPushAgentConfiguration()
    {
        pushAgentConfigurationParameters = new PushAgentConfigurationParameters();

        try
        {
            LoadMQTTConfiguration();

            pushAgentConfigurationParameters.datalogger = GetDataLogger();
            pushAgentConfigurationParameters.pushFullSample = LogicObject.GetVariable("PushFullSample").Value;
            pushAgentConfigurationParameters.preserveDataLoggerHistory = LogicObject.GetVariable("PreserveDataLoggerHistory").Value;
        }
        catch (Exception e)
        {
            throw new CoreConfigurationException("PushAgent: Configuration error");
        }

    }

    private void CheckMQTTParameters()
    {
        if (pushAgentConfigurationParameters.mqtttConfigurationParameters.useSSL)
        {
            var pathCACert = pushAgentConfigurationParameters.mqtttConfigurationParameters.pathCACert;
            var pathClientCert = pushAgentConfigurationParameters.mqtttConfigurationParameters.pathClientCert;
            if (string.IsNullOrEmpty(pathCACert) || string.IsNullOrEmpty(pathClientCert))
            {
                var username = pushAgentConfigurationParameters.mqtttConfigurationParameters.username;
                var password = pushAgentConfigurationParameters.mqtttConfigurationParameters.password;
                if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
                    useIoTHub = true;
                else
                    Log.Warning("PushAgent", "Path of CA or ClientCertificate missing.");
            }
        }

        var qos = pushAgentConfigurationParameters.mqtttConfigurationParameters.qos;
        if (qos < 0 || qos > 2)
            Log.Warning("PushAgent", "QoS Values valid are 0, 1, 2");

        if (useIoTHub)
        {
            // we modify the topic because IoTHub does not have / at the start in the topic name
            pushAgentConfigurationParameters.mqtttConfigurationParameters.brokerTopic = pushAgentConfigurationParameters.mqtttConfigurationParameters.brokerTopic.Remove(0, 1);
            if (pushAgentConfigurationParameters.mqtttConfigurationParameters.qos == 2)
            {
                Log.Warning("PushAgent", "QoS level 2 (EXACLTY_ONCE) is NOT supported by IoTHub. Setting QoS level to 1 (AT LEAST ONCE)");
                pushAgentConfigurationParameters.mqtttConfigurationParameters.qos = 1;
            }
        }
    }

    private int GetMaximumRecordsPerPacket()
    {
        return LogicObject.GetVariable("MaximumItemsPerPacket").Value;
    }

    private int GetMaximumPublishTime()
    {
        return LogicObject.GetVariable("MaximumPublishTime").Value;
    }

    private int GetMinimumPublishTime()
    {
        return LogicObject.GetVariable("MinimumPublishTime").Value;
    }

    private DataLogger GetDataLogger()
    {
        var dataLoggeeNodeId = LogicObject.GetVariable("DataLogger").Value;
        return InformationModel.Get<DataLogger>(dataLoggeeNodeId);
    }

    private string ResourceUriValueToAbsoluteFilePath(UAValue value)
    {
        var resourceUri = new ResourceUri(value);
        return resourceUri.Uri;
    }

    private string GetDataLoggerTableName()
    {
        if (pushAgentConfigurationParameters.datalogger.TableName != null)
            return pushAgentConfigurationParameters.datalogger.TableName;

        return pushAgentConfigurationParameters.datalogger.BrowseName;
    }

    private void CreatePushAgentStore(string browsename, string filename)
    {
        try
        {
            SQLiteStore store = InformationModel.MakeObject<SQLiteStore>(browsename);
            store.Filename = filename;
            LogicObject.Add(store);
        }
        catch (Exception e)
        {
            throw new Exception("Unable to create support store " + e.Message);
        }
    }

    private bool dataFetchTaskRunning = false;
    private bool restartDataFetchTaskRunning = false;
    private bool initialized = false;
    private bool useIoTHub;
    private bool closing = false;
    private bool insertOpCode;
    private bool insertVariableTimestamp;
    private int nextRestartTimeout;
    private Packet pendingSendPacket;
    private DelayedTask restartDataFetchTask;
    private DelayedTask dataFetchTask;
    private LongRunningTask onAfterPublishTask;
    private AutoResetEvent onPublishedEvent;
    private PushAgentConfigurationParameters pushAgentConfigurationParameters;
    private MQTTConnector mqttClientConnector;
    private SupportStore pushAgentStore;
    private DataLoggerStoreWrapper dataLoggerStore;
    private DataLoggerStatusStoreWrapper statusStoreWrapper;
    private JSONBuilder jsonCreator;
    DataLoggerRecordPuller dataLoggerRecordPuller;

    class MQTTConfigurationParameters
    {
        public string clientId;
        public string brokerIPAddress;
        public int brokerPort;
        public string brokerTopic;
        public int qos;
        public bool useSSL;
        public string pathClientCert;
        public string passwordClientCert;
        public string pathCACert;
        public string username;
        public string password;
    }

    class PushAgentConfigurationParameters
    {
        public MQTTConfigurationParameters mqtttConfigurationParameters;
        public DataLogger datalogger;
        public bool pushFullSample;
        public bool preserveDataLoggerHistory;
    }

    private void FirstConfiguration()
    {

    }

    private static Random random = new Random();

    [ExportMethod]
    public void AttachNewMachine(string machineName)
    {
        if (MqttConnectorInstance.mqttconn == null)
        {
            ConfigureMQTT();

            onPublishedEvent = new AutoResetEvent(false);
            onAfterPublishTask = new LongRunningTask(OnAfterMessagePublished, LogicObject);
            onAfterPublishTask.Start();
            StartFetchTimer();
            initialized = true;
            temperature.VariableChange += TimeToFinish_VariableChange;
            timeToFinish.VariableChange += TimeToFinish_VariableChange;
            pTCheckConnection = new PeriodicTask(CheckMqttConn, 1000, LogicObject);
            pTCheckConnection.Start();

        }
        else
        {
            Project.Current.GetVariable("Model/ChangeMachineName").Value++;
        }
            


        //if (MqttConnectorInstance.mqttconn == null)
        //{
        //    try
        //    {
        //        //CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("en-US");
        //        //LoadPushAgentConfiguration();
        //        //ConfigureStores();
        //        //ConfigureDataLoggerRecordPuller();
        //        ConfigureMQTT();

        //        onPublishedEvent = new AutoResetEvent(false);
        //        onAfterPublishTask = new LongRunningTask(OnAfterMessagePublished, LogicObject);
        //        onAfterPublishTask.Start();
        //        StartFetchTimer();
        //        initialized = true;
        //        temperature.VariableChange += TimeToFinish_VariableChange;
        //        timeToFinish.VariableChange += TimeToFinish_VariableChange;
        //        pTCheckConnection = new PeriodicTask(CheckMqttConn, 1000, LogicObject);
        //        pTCheckConnection.Start();
        //    }
        //    catch (Exception e)
        //    {
        //        Log.Warning("PushAgent", "Unable to initialize, an error occurred: " + e.Message);
        //    }
        //}
        //else
        //{
        //    if (MqttConnectorInstance.mqttconn.Connected())
        //    {
        //        //MqttConnectorInstance.mqttconn.clientID = (string)Project.Current.GetVariable("Model/RetentiveMachineData/MachineName").Value;
        //        MqttConnectorInstance.mqttconn.mqttClient.Disconnect();
        //        //Thread.Sleep(5000);
        //        MqttConnectorInstance.mqttconn.clientID = (string)Project.Current.GetVariable("Model/RetentiveMachineData/MachineName").Value;
        //        MqttConnectorInstance.mqttconn.Connect();
        //        //onPublishedEvent = new AutoResetEvent(false);
        //        //onAfterPublishTask = new LongRunningTask(OnAfterMessagePublished, LogicObject);
        //        //onAfterPublishTask.Start();
        //        //StartFetchTimer();
        //        //initialized = true;
        //    }
        //    else
        //    {
        //        MqttConnectorInstance.mqttconn.clientID = (string)Project.Current.GetVariable("Model/RetentiveMachineData/MachineName").Value;
        //        MqttConnectorInstance.mqttconn.Connect();
        //        //onPublishedEvent = new AutoResetEvent(false);
        //        //onAfterPublishTask = new LongRunningTask(OnAfterMessagePublished, LogicObject);
        //        //onAfterPublishTask.Start();
        //        //StartFetchTimer();
        //        //initialized = true;
        //    }
        //}


        //if (MqttConnectorInstance.mqttconn != null)
        //{
        //    Stop();
        //    Thread.Sleep(5000);
        //    try
        //    {
        //        LoadPushAgentConfiguration();
        //        CheckMQTTParameters();

        //        //ConfigureStores();
        //        //ConfigureDataLoggerRecordPuller();
        //        ConfigureMQTT();

        //        //onPublishedEvent = new AutoResetEvent(false);
        //        //onAfterPublishTask = new LongRunningTask(OnAfterMessagePublished, LogicObject);
        //        //onAfterPublishTask.Start();

        //        StartFetchTimer();
        //        initialized = true;
                
        //        temperature.VariableChange += TimeToFinish_VariableChange;
        //        timeToFinish.VariableChange += TimeToFinish_VariableChange;
        //        pTCheckConnection = new PeriodicTask(CheckMqttConn, 1000, LogicObject);
        //        pTCheckConnection.Start();
        //    }
        //    catch (Exception e)
        //    {
        //        Log.Warning("PushAgent", "Unable to initialize, an error occurred: " + e.Message);
        //    }
        //}
        //else
        //{
        //    //CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("en-US");
        //    try
        //    {
        //        LoadPushAgentConfiguration();
        //        CheckMQTTParameters();

        //        //ConfigureStores();
        //        //ConfigureDataLoggerRecordPuller();
        //        ConfigureMQTT();

        //        //onPublishedEvent = new AutoResetEvent(false);
        //        //onAfterPublishTask = new LongRunningTask(OnAfterMessagePublished, LogicObject);
        //        //onAfterPublishTask.Start();

        //        StartFetchTimer();
        //        initialized = true;
        //        temperature.VariableChange += TimeToFinish_VariableChange;
        //        timeToFinish.VariableChange += TimeToFinish_VariableChange;
        //        pTCheckConnection = new PeriodicTask(CheckMqttConn, 1000, LogicObject);
        //        pTCheckConnection.Start();
        //    }
        //    catch (Exception e)
        //    {
        //        Log.Warning("PushAgent", "Unable to initialize, an error occurred: " + e.Message);
        //    }
        //}

        Thread.Sleep(10000);

        string message = @"{ ""Name"":""" + machineName + @""",""Country"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineCountry").Value + @""",""State"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineState").Value + @""",""City"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineCity").Value + @""",""Address"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineAddress").Value + @""",""CivicNumber"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineCivicNumber").Value + @""",""Latitude"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineLatitude").Value + @""",""Longitude"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineLongitude").Value + @"""}";
        mqttClientConnector.Publish(message, "/iotdemo-machine", false, 2);
    }

    [ExportMethod]
    public void SendChatMessage(string machineName, string messageText)
    {
        string message = @"{ ""MachineName"":""" + machineName + @""",""Message"":""" + messageText + @"""}";
        mqttClientConnector.Publish(message, "/iotdemo-chatmessage", false, 2);
    }

    public void PushNewJob()
    {
        string message = @"{ ""MachineName"":""" + Project.Current.GetVariable("Model/RetentiveMachineData/MachineName").Value + @""",""Start"":""" + Project.Current.GetVariable("Model/Job/Start").Value + @""",""Finish"":""" + Project.Current.GetVariable("Model/Job/Finish").Value + @""",""RecipeName"":""" + Project.Current.GetVariable("Model/Job/Recipe").Value + @""",""LPGConsumption"":""" + Project.Current.GetVariable("Model/Job/LPGConsumption").Value + @""",""ElectricalConsumption"":""" + Project.Current.GetVariable("Model/Job/ElectricalConsumption").Value + @"""}";
        mqttClientConnector.Publish(message, "/iotdemo-job", false, 2);
    }
}

public static class MqttConnectorInstance
{
    public static MQTTConnector mqttconn;
}
