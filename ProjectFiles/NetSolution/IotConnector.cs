#region Using directives
using System;
using FTOptix.Core;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.CoreBase;
using FTOptix.Store;
using FTOptix.NetLogic;
using FTOptix.UI;
using FTOptix.HMIProject;
using FTOptix.DataLogger;
using FTOptix.Alarm;
using FTOptix.CommunicationDriver;
using FTOptix.Recipe;
using FTOptix.SQLiteStore;
using FTOptix.OPCUAServer;
using FTOptix.NativeUI;
using FTOptix.EventLogger;
using FTOptix.Retentivity;
using FTOptix.Modbus;
#endregion

public class IotConnector : BaseNetLogic
{
    public override void Start()
    {
        // Insert code to be executed when the user-defined logic is started
    }

    public override void Stop()
    {
        // Insert code to be executed when the user-defined logic is stopped
    }
}
