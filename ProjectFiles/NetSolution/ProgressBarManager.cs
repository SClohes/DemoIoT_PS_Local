#region Using directives
using System;
using FTOptix.Core;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.CoreBase;
using FTOptix.NetLogic;
using FTOptix.UI;
using FTOptix.HMIProject;
using FTOptix.Alarm;
using FTOptix.CommunicationDriver;
using FTOptix.OPCUAServer;
using FTOptix.EventLogger;
using FTOptix.NativeUI;
using FTOptix.DataLogger;
using FTOptix.Store;
using FTOptix.SQLiteStore;
using FTOptix.Recipe;
using FTOptix.Retentivity;
using FTOptix.Modbus;
#endregion

public class ProgressBarManager : BaseNetLogic
{
    public override void Start()
    {
        //var progress = Project.Current.GetVariable("Model/Phases/ProgressBarPhase3");
        //progress.VariableChange += Progress_VariableChange;
    }

    private void Progress_VariableChange(object sender, VariableChangeEventArgs e)
    {
        var chargeSetPoint = Project.Current.GetVariable("Model/Phases/PhasesSetpointPercent");
        var progressPercentage = ((IUAVariable)sender).Value / (double)chargeSetPoint.Value;
        Rectangle progressBar = (Rectangle)Owner;

        if (progressPercentage < 0.75)
        {
            progressBar.FillColor = Colors.LightSkyBlue;
        }
        else if (progressPercentage < 0.99)
        {
            progressBar.FillColor = Colors.LightSkyBlue;
        }
        else
        {
            progressBar.FillColor = Colors.LightSkyBlue;
        }
    }

    public override void Stop()
    {
        // Insert code to be executed when the user-defined logic is stopped
    }
}
