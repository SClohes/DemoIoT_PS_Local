#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.HMIProject;
using FTOptix.OPCUAServer;
using FTOptix.UI;
using FTOptix.NativeUI;
using FTOptix.CoreBase;
using FTOptix.NetLogic;
using FTOptix.Alarm;
using System.Collections.Generic;
using System.Globalization;
//using static uPLibrary.Networking.M2Mqtt.MqttClient;
//using System.Security.Cryptography.X509Certificates;
//using uPLibrary.Networking.M2Mqtt;
//using uPLibrary.Networking.M2Mqtt.Messages;
using FTOptix.Core;
using FTOptix.DataLogger;
using System.Text;
using System.IO;
using Newtonsoft.Json;
using FTOptix.ODBCStore;
using FTOptix.Recipe;
using FTOptix.Store;
using FTOptix.SQLiteStore;
using FTOptix.CommunicationDriver;
using FTOptix.Retentivity;
using FTOptix.Modbus;
#endregion

public class DigitalAlarmWithPushNotificationLogic : BaseNetLogic, IUAEventObserver
{
    class EventData
    {
        public EventData(IUAObject eventNotifier, IUAObjectType eventType, IReadOnlyList<object> args)
        {
            EventNotifier = eventNotifier;
            EventType = eventType;
            Args = args;
        }

        public IUAObject EventNotifier { get; set; }
        public IUAObjectType EventType { get; set; }
        public IReadOnlyList<object> Args { get; set; }
    }

    public override void Start()
    {
        //CultureInfo.DefaultThreadCurrentCulture = new System.Globalization.CultureInfo("en-US");

        //try
        //{
        //    LoadPushAgentSubscriberConfiguration();
        //    ConfigureMQTT();
        //}
        //catch (Exception e)
        //{
        //    Log.Warning("PushAgentSubscriber", "Unable to initialize, an error occurred: " + e.Message);
        //}

        //// Add subscriber
        //mqttClientConnector.AddSubscriber(subscriberConfigurationParameters.mqtttConfigurationParameters.brokerTopic, 1, SubscribeClientMqttMsgPublishReceived);
        //System.Threading.Thread.Sleep(5000);

        alarmObject = (AlarmController)Owner;
        previousActiveState = GetInitialActiveState();
        affinityId = LogicObject.Context.AssignAffinityId();
        RegisterForLocalizedEvents();

        //emailUserNode = GetAlarmProperty("EmailUser");
        //if (emailUserNode == null)
        //    return;

        //var emailVariable = emailUserNode.GetVariable("Email");
        //if (emailVariable == null)
        //{
        //    Log.Error("DigitalAlarmWithEmailNotificationLogic", $"Could not find Email variable of {emailUserNode.BrowseName} user");
        //    return;
        //}

        //email = emailVariable.Value;
        //if (string.IsNullOrEmpty(email))
        //{
        //    Log.Error("DigitalAlarmWithEmailNotificationLogic", $"Email variable property missing in user {Log.Node(emailUserNode)} set in {Log.Node(alarmObject)}");
        //    return;
        //}
    }

    private void RegisterForLocalizedEvents()
    {
        systemUserSessionHanlders = new Dictionary<string, ISessionHandler>();
        var projectLocales = Project.Current.Locales;

        using (var destroyOnExit = LogicObject.Context.Sessions.ImpersonateRootTemporary())
        {
            foreach (var locale in projectLocales)
            {
                var sessionHandler = LogicObject.Context.Sessions.InternalCreateSession(
                    new QualifiedName(2, "SystemUser_" + locale), NodeId.Random(Project.Current.NodeId.NamespaceIndex));
                systemUserSessionHanlders.Add(locale, sessionHandler);
            }
        }

        eventRegistrations = new List<IEventRegistration>();

        foreach (var locale in projectLocales)
        {
            var sessionHandler = systemUserSessionHanlders[locale];
            using (var destroyOnExit = LogicObject.Context.Sessions.ImpersonateSessionTemporary(sessionHandler))
            {
                eventRegistrations.Add(alarmObject.RegisterUAEventObserver(this, OpcUa.ObjectTypes.AlarmConditionType, affinityId));
            }
        }
    }

    public override void Stop()
    {
        foreach (var registration in eventRegistrations)
            registration?.Dispose();

        foreach (var sessionHandler in systemUserSessionHanlders.Values)
            sessionHandler?.Dispose();
    }

    public void OnEvent(IUAObject eventNotifier, IUAObjectType eventType, IReadOnlyList<object> args, ulong senderId)
    {
        var eventArguments = eventType.EventArguments;
        var eventId = (ByteString)eventArguments.GetFieldValue(args, "EventId");
        var eventIdString = eventId.ToString();

        if (!receivedEvents.ContainsKey(eventIdString))
        {
            var eventList = new List<EventData>
            {
                new EventData(eventNotifier, eventType, args)
            };
            receivedEvents.Add(eventIdString, eventList);
        }
        else
        {
            receivedEvents[eventIdString].Add(new EventData(eventNotifier, eventType, args));
        }

        if (receivedEvents[eventIdString].Count == Project.Current.Locales.Length)
        {
            try
            {
                var alarmMessage = ConstructAlarmMessage(eventId);
                PushAlarmDatas(alarmMessage);
            }
            catch(Exception e)
            {
                Log.Error(e.Message);
            }
        }
    }

    private void PushAlarmDatas(string alarmMessage)
    {
        NetLogicObject pushAgent = Project.Current.Find<NetLogicObject>("PushAgentAlarmsRecipes");
        var args = new string[] { alarmMessage };
        pushAgent.ExecuteMethod("PushAlarm", args);
        //Log.Info("Messaggio Inviato: " + alarmMessage);
    }

    private bool GetInitialActiveState()
    {
        var retainedAlarmsNode = InformationModel.Get(FTOptix.Alarm.Objects.RetainedAlarms);

        var retainedAlarm = retainedAlarmsNode.Find(alarmObject.BrowseName);
        if (retainedAlarm == null)
            return false;

        return retainedAlarm.GetVariable("ActiveState/Id").Value;
    }

    private bool IsAlarmTransitioningFromInactiveState(bool currentActiveState)
    {
        if (!previousActiveState && currentActiveState)
        {
            previousActiveState = currentActiveState;
            return true;
        }

        previousActiveState = currentActiveState;
        return false;
    }

    private string ConstructAlarmMessage(ByteString eventId)
    {
        var currentEventList = receivedEvents[eventId.ToString()];
        var eventArguments = currentEventList[0].EventType.EventArguments;
        var args = currentEventList[0].Args;

        var timestamp = eventArguments.GetFieldValue(args, "Time");
        var ackedState = eventArguments.GetFieldValue(args, "AckedState/Id");
        var confirmedState = eventArguments.GetFieldValue(args, "ConfirmedState/Id");
        var activeState = eventArguments.GetFieldValue(args, "ActiveState/Id");
        var enabledState = eventArguments.GetFieldValue(args, "EnabledState/Id");
        var conditionName = eventArguments.GetFieldValue(args, "ConditionName");
        var sourceName = (string)Project.Current.GetVariable("Model/RetentiveMachineData/MachineName").Value;//(string)MqttConnectorInstance.mqttconn.clientID;
        var severity = eventArguments.GetFieldValue(args, "Severity");
        var localTime = eventArguments.GetFieldValue(args, "LocalTime");

        var sb = new StringBuilder();
        var sw = new StringWriter(sb);
        using (var writer = new JsonTextWriter(sw))
        {
            writer.Formatting = Formatting.None;

            writer.WriteStartObject();
            writer.WritePropertyName("ConditionName");
            writer.WriteValue(conditionName);
            writer.WritePropertyName("Time");
            writer.WriteValue(timestamp);
            writer.WritePropertyName("ActiveState_Id");
            writer.WriteValue(activeState);
            writer.WritePropertyName("AckedState_Id");
            writer.WriteValue(ackedState);
            writer.WritePropertyName("ConfirmedState_Id");
            writer.WriteValue(confirmedState);
            writer.WritePropertyName("EnabledState_Id");
            writer.WriteValue(enabledState);
            writer.WritePropertyName("SourceName");
            writer.WriteValue(sourceName);
            writer.WritePropertyName("Severity");
            writer.WriteValue(severity);
            writer.WritePropertyName("LocalTime");
            writer.WriteValue(((Struct)localTime).Values[0]);

            foreach (var evt in currentEventList)
            {
                var message = (LocalizedText)evt.EventType.EventArguments.GetFieldValue(evt.Args, "Message");
                writer.WritePropertyName("Message_" + message.LocaleId);
                writer.WriteValue(message.Text);
            }

            writer.WriteEnd();
        }

        return sb.ToString();
    }

    private void SendAlarmEmail(string alarmBrowseName, string alarmMessage)
    {
        var emailSender = GetAlarmProperty("EmailSender") as NetLogicObject;
        if (emailSender == null)
        {
            Log.Error("DigitalAlarmWithEmailNotificationLogic", "Could not send email: Invalid or missing EmailSender NetLogic");
            return;
        }

        Log.Info("DigitalAlarmWithEmailNotificationLogic", $"Sending email to {email}");
        var args = new string[] { email, alarmBrowseName, alarmMessage };
        emailSender.ExecuteMethod("SendEmail", args);
    }

    private IUANode GetAlarmProperty(string propertyName)
    {
        var property = alarmObject.GetVariable(propertyName);
        if (property == null)
        {
            Log.Error("DigitalAlarmWithEmailNotificationLogic", $"Alarm property {propertyName} could not be found");
            return null;
        }

        var propertyValue = (NodeId)property.Value;
        if (propertyValue == NodeId.Empty || propertyValue == null)
        {
            Log.Error("DigitalAlarmWithEmailNotificationLogic", $"Invalid or missing value for alarm property {propertyName}");
            return null;
        }

        var pointedNode = InformationModel.Get(propertyValue);
        if (pointedNode == null)
        {
            Log.Error("DigitalAlarmWithEmailNotificationLogic", $"Could not resolve alarm property {propertyName}");
            return null;
        }

        return pointedNode;
    }

    private string email;
    private bool previousActiveState;

    private List<IEventRegistration> eventRegistrations;
    private Dictionary<string, ISessionHandler> systemUserSessionHanlders;
    private Dictionary<string, List<EventData>> receivedEvents = new Dictionary<string, List<EventData>>();
    private uint affinityId;
    private IUANode emailUserNode;
    private IUAObject alarmObject;

    //private void ConfigureMQTT()
    //{
    //    var username = subscriberConfigurationParameters.mqtttConfigurationParameters.username;
    //    var password = subscriberConfigurationParameters.mqtttConfigurationParameters.password;
    //    if ((useIoTHub && !string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password)) ||
    //        (!useIoTHub && !string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password)))
    //    {
    //        // IoTHub or classic username and password authentication
    //        mqttClientConnector = new MQTTConnector(LogicObject,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.brokerIPAddress,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.clientId,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.username,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.password,
    //                                                useIoTHub,
    //                                                null,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.brokerPort);
    //    }
    //    else if (subscriberConfigurationParameters.mqtttConfigurationParameters.useSSL)
    //    {
    //        // SSL authentication
    //        mqttClientConnector = new MQTTConnector(LogicObject,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.brokerIPAddress,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.clientId,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.pathClientCert,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.passwordClientCert,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.pathCACert,
    //                                                null,
    //                                                subscriberConfigurationParameters.mqtttConfigurationParameters.brokerPort);
    //    }
    //    else
    //    {
    //        // Anonymous authentication
    //        mqttClientConnector = new MQTTConnector(LogicObject,
    //                                    subscriberConfigurationParameters.mqtttConfigurationParameters.brokerIPAddress,
    //                                    subscriberConfigurationParameters.mqtttConfigurationParameters.clientId,
    //                                    null,
    //                                    subscriberConfigurationParameters.mqtttConfigurationParameters.brokerPort);
    //    }
    //}

    //private void SubscribeClientMqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
    //{
    //    var messageVariable = Project.Current.GetVariable("Model/Message");
    //    messageVariable.Value = System.Text.Encoding.UTF8.GetString(e.Message);

    //}

    //private void LoadMQTTConfiguration()
    //{
    //    subscriberConfigurationParameters.mqtttConfigurationParameters = new MQTTConfigurationParameters
    //    {
    //        clientId = LogicObject.GetVariable("ClientId").Value,
    //        brokerIPAddress = LogicObject.GetVariable("BrokerIPAddress").Value,
    //        brokerPort = LogicObject.GetVariable("BrokerPort").Value,
    //        brokerTopic = "/" + LogicObject.GetVariable("BrokerTopic").Value,
    //        qos = LogicObject.GetVariable("QoS").Value,
    //        useSSL = LogicObject.GetVariable("UseSSL").Value,
    //        pathCACert = ResourceUriValueToAbsoluteFilePath(LogicObject.GetVariable("UseSSL/CACert").Value),
    //        pathClientCert = ResourceUriValueToAbsoluteFilePath(LogicObject.GetVariable("UseSSL/ClientCert").Value),
    //        passwordClientCert = LogicObject.GetVariable("UseSSL/ClientCertPassword").Value,
    //        username = LogicObject.GetVariable("Username").Value,
    //        password = LogicObject.GetVariable("Password").Value
    //    };
    //}

    //private string ResourceUriValueToAbsoluteFilePath(UAValue value)
    //{
    //    var resourceUri = new ResourceUri(value);
    //    return resourceUri.Uri;
    //}

    //private void LoadPushAgentSubscriberConfiguration()
    //{
    //    subscriberConfigurationParameters = new SubscriberConfigurationParameters();

    //    try
    //    {
    //        LoadMQTTConfiguration();
    //    }
    //    catch (Exception e)
    //    {
    //        throw new CoreConfigurationException("PushAgent: Configuration error");
    //    }
    //}

    //private string messages;
    //private bool useIoTHub = false;
    //private DataLogger dataLogger;
    //private MQTTConnector mqttClientConnector;
    //private SubscriberConfigurationParameters subscriberConfigurationParameters;

    //class MQTTConfigurationParameters
    //{
    //    public string clientId;
    //    public string brokerIPAddress;
    //    public int brokerPort;
    //    public string brokerTopic;
    //    public int qos;
    //    public bool useSSL;
    //    public string pathClientCert;
    //    public string passwordClientCert;
    //    public string pathCACert;
    //    public string username;
    //    public string password;
    //}

    //class SubscriberConfigurationParameters
    //{
    //    public MQTTConfigurationParameters mqtttConfigurationParameters;
    //}

    //[ExportMethod]
    //public void Method1()
    //{
    //    mqttClientConnector.Publish("ciao", "/my_alarm_topic", true, 2);
    //}
}
