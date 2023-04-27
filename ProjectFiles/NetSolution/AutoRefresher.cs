#region Using directives
using System;
using FTOptix.Core;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.Store;
using FTOptix.OPCUAServer;
using FTOptix.UI;
using FTOptix.CoreBase;
using FTOptix.SQLiteStore;
using FTOptix.NativeUI;
using FTOptix.HMIProject;
using FTOptix.NetLogic;
using System.Threading;
using FTOptix.OPCUAClient;
using FTOptix.DataLogger;
using FTOptix.CommunicationDriver;
using FTOptix.Modbus;
#endregion

public class AutoRefresher : BaseNetLogic
{
    DataGrid dataGrid;
    public override void Start()
    {
        var autoRefreshCheckBox = LogicObject.Owner.Owner.Get<CheckBox>("CheckBox1");
        var activeVariable = autoRefreshCheckBox.CheckedVariable;
        activeVariable.VariableChange += OnActiveVariableChanged;
    }

    private void OnActiveVariableChanged(object sender, VariableChangeEventArgs e)
    {
        if ((bool)e.NewValue)
        {
            dataGrid = (DataGrid)Owner;
            refreshTask = new PeriodicTask(RefreshDataGrid, 4000, LogicObject);
            refreshTask.Start();
        }
        else
        {
            refreshTask?.Dispose();
        }
    }

    public override void Stop()
    {
        refreshTask?.Dispose();
    }

    public void RefreshDataGrid()
    {
        Thread.Sleep(1000);
        dataGrid.Refresh();
    }

    private PeriodicTask refreshTask;
}
