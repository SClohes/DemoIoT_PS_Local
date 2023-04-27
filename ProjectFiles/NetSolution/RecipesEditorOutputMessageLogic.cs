#region StandardUsing
using System;
using FTOptix.Core;
using FTOptix.CoreBase;
using FTOptix.HMIProject;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.NetLogic;
using FTOptix.UI;
using FTOptix.Recipe;
using FTOptix.OPCUAServer;
using FTOptix.Store;
using System.Timers;
using FTOptix.Retentivity;
using FTOptix.NativeUI;
using FTOptix.Modbus;
using FTOptix.CommunicationDriver;
using FTOptix.EthernetIP;
#endregion

public class RecipesEditorOutputMessageLogic : FTOptix.NetLogic.BaseNetLogic
{
    public override void Start()
    {
        messageVariable = Owner.Children.Get<IUAVariable>("Message");
        if (messageVariable == null)
            throw new ArgumentNullException("Unable to find variable Message in OutputMessage label");
    }

	public override void Stop()
    {
        lock (lockObject)
        {
            task?.Dispose();
        }
	}

	[ExportMethod]
	public void SetOutputMessage(string message)
	{
        lock (lockObject)
        {
            task?.Dispose();

            messageVariable.Value = message;
            task = new DelayedTask(() => { messageVariable.Value = ""; }, 5000, LogicObject);
            task.Start();
        }
	}
    
	[ExportMethod]
	public void SetOutputLocalizedMessage(LocalizedText message)
	{
        SetOutputMessage(InformationModel.LookupTranslation(message).Text);
	}

	DelayedTask task;
	IUAVariable messageVariable;
    object lockObject = new object();
}
