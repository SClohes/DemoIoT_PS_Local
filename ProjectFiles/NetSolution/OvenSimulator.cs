#region Using directives
using System;
using FTOptix.Core;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.HMIProject;
using FTOptix.UI;
using FTOptix.NativeUI;
using FTOptix.OPCUAServer;
using FTOptix.CoreBase;
using FTOptix.NetLogic;
using System.Threading;
using System.Threading.Tasks;
using FTOptix.DataLogger;
using FTOptix.Store;
using FTOptix.SQLiteStore;
using FTOptix.Recipe;
using FTOptix.CommunicationDriver;
using FTOptix.Retentivity;
using FTOptix.Modbus;
using FTOptix.EthernetIP;
#endregion

public class OvenSimulator : BaseNetLogic
{
    private PeriodicTask updateOvenTemperaturePeriodicTask;
    private int temperatureUpdateCounter = 0;
    private PeriodicTask periodicTask;
    IUAVariable spF1, durationF1, speedFanInputF1, speedFanOutputF1, actualDurationF1;
    IUAVariable spF2, durationF2, speedFanInputF2, speedFanOutputF2, actualDurationF2;
    IUAVariable spF3, durationF3, speedFanInputF3, speedFanOutputF3, actualDurationF3;
    IUAVariable actualDuration, startStop, running, actualPhase, actualTemperature, actualSetPoint;
    IUAVariable speedFanInput, speedFanOutput, timeToFinishPhase, timeToFinishAll;
    IUAVariable jobsCounter, preheating, preheatingTemperature, gas, kWh;
    IUAVariable jobStart, jobFinish, jobRecipe, jobGas, jobkWh;
    IUAVariable gasThrottle, kW;
    IUAVariable useMinute, totEE, totLPG;
    IUAVariable filterAlarm, temperatureAlarm, doorAlarm, fanAlarm, filterLimit;
    private int phaseSetPoint;
    private double phaseDuration, allDuration;
    private int ovenLogicCycles;
    private bool ovenRunning;
    private int currentSetpoint;
    private double currentTemperature;
    private bool preheat;
    private int preheatTemp;
    private Random rnd;
    private double istantkW;
    private double throttle;
    private double temp_kwh;
    private double temp_gas;
    private int useMinuteCounter;

    public override void Start()
    {
        rnd = new Random();
        filterAlarm = Project.Current.GetVariable("Model/Alarms/FilterAlarm");
        temperatureAlarm = Project.Current.GetVariable("Model/Alarms/TemperatureAlarm");
        doorAlarm = Project.Current.GetVariable("Model/Alarms/DoorAlarm");
        fanAlarm = Project.Current.GetVariable("Model/Alarms/FanAlarm");
        useMinute = Project.Current.GetVariable("Model/RetentiveMachineData/UseMinute");
        filterLimit = Project.Current.GetVariable("Model/RetentiveMachineData/FilterLimit");
        useMinuteCounter = useMinute.Value;
        spF1 = LogicObject.GetVariable("SetPointF1");
        durationF1 = LogicObject.GetVariable("DurationF1");
        speedFanInputF1 = LogicObject.GetVariable("SpeedFanInputF1");
        speedFanOutputF1 = LogicObject.GetVariable("SpeedFanOutputF1");
        actualDurationF1 = LogicObject.GetVariable("ActualDurationF1");
        spF2 = LogicObject.GetVariable("SetPointF2");
        durationF2 = LogicObject.GetVariable("DurationF2");
        speedFanInputF2 = LogicObject.GetVariable("SpeedFanInputF2");
        speedFanOutputF2 = LogicObject.GetVariable("SpeedFanOutputF2");
        actualDurationF2 = LogicObject.GetVariable("ActualDurationF2");
        spF3 = LogicObject.GetVariable("SetPointF3");
        durationF3 = LogicObject.GetVariable("DurationF3");
        speedFanInputF3 = LogicObject.GetVariable("SpeedFanInputF3");
        speedFanOutputF3 = LogicObject.GetVariable("SpeedFanOutputF3");
        actualDurationF3 = LogicObject.GetVariable("ActualDurationF3");
        actualDuration = LogicObject.GetVariable("ActualDuration");
        actualSetPoint = LogicObject.GetVariable("ActualSetPoint");
        startStop = LogicObject.GetVariable("StartStop");
        running = LogicObject.GetVariable("Running");
        actualPhase = LogicObject.GetVariable("ActualPhase");
        actualTemperature = LogicObject.GetVariable("ActualTemperature");
        speedFanInput = LogicObject.GetVariable("SpeedFanInput");
        speedFanOutput = LogicObject.GetVariable("SpeedFanOutput");
        timeToFinishPhase = LogicObject.GetVariable("TimeToFinishPhase");
        timeToFinishAll = LogicObject.GetVariable("TimeToFinishAll");
        jobsCounter = Project.Current.GetVariable("Model/RetentiveMachineData/CycleNumber");// LogicObject.GetVariable("JobsCounter");
        totEE = Project.Current.GetVariable("Model/RetentiveMachineData/TotEE");// LogicObject.GetVariable("JobsCounter");
        totLPG = Project.Current.GetVariable("Model/RetentiveMachineData/TotLPG");// LogicObject.GetVariable("JobsCounter");
        //jobsCounter = Project.Current.GetVariable("Model/RetentiveMachineData/CycleNumber");
        preheating = LogicObject.GetVariable("Preheating");
        preheatingTemperature = LogicObject.GetVariable("PreheatingTemperature");
        gas = LogicObject.GetVariable("Gas");
        kWh = LogicObject.GetVariable("kWh");
        jobStart = LogicObject.GetVariable("JobStart");
        jobFinish = LogicObject.GetVariable("JobFinish");
        jobkWh = LogicObject.GetVariable("JobkWh");
        jobGas = LogicObject.GetVariable("JobGas");
        jobRecipe = LogicObject.GetVariable("JobRecipe");
        gasThrottle = LogicObject.GetVariable("GasThrottle");
        kW = LogicObject.GetVariable("kW");
        preheat = preheating.Value;
        preheatTemp = preheatingTemperature.Value;
        currentTemperature = (double)actualTemperature.Value;
        durationF1.Value = 15;
        durationF2.Value = 20;
        durationF3.Value = 12;
        startStop.VariableChange += StartStop_VariableChange;
        ovenRunning = running.Value;
        useMinute.VariableChange += UseMinute_VariableChange;
        updateOvenTemperaturePeriodicTask = new PeriodicTask(UpdateOvenTemperature, 250, LogicObject);
        updateOvenTemperaturePeriodicTask.Start();
    }

    private void UseMinute_VariableChange(object sender, VariableChangeEventArgs e)
    {
        if (useMinute.Value > filterLimit.Value)
            filterAlarm.Value = true;
        else
            filterAlarm.Value = false;
    }

    private void StartStop_VariableChange(object sender, VariableChangeEventArgs e)
    {
        if (!running.Value && startStop.Value)
        {
            StartOven();
        }
        if (!startStop.Value && running.Value)
        {
            StopOven();
        }
    }

    public override void Stop()
    {
    }

    private void StopOven()
    {
        running.Value = false;
        ovenRunning = false;
        startStop.Value = false;
        actualPhase.Value = 0;
        speedFanInput.Value = 0;
        speedFanOutput.Value = 0;
        kWh.Value = kWh.Value + (kW.Value * durationF3.Value) / 3600.0;
        periodicTask?.Dispose();
        periodicTask = null;
    }

    private void ResetOven()
    {
        actualDurationF1.Value = 0;
        actualDurationF2.Value = 0;
        actualDurationF3.Value = 0;
        actualDuration.Value = 0;
        speedFanInput.Value = 0;
        speedFanOutput.Value = 0;
        timeToFinishAll.Value = 0;
        gas.Value = 0.0;
        kWh.Value = 0.0;
        temp_kwh = 0;
        temp_gas = 0;
    }

    private void SetPointPhase(int phase)
    {
        if (phase < 4)
        {
            actualPhase.Value = phase;
            switch (phase)
            {
                case 1:
                    //kWh.Value = (kW.Value * phaseDuration) / 3600.0;
                    speedFanInput.Value = speedFanInputF1.Value;
                    speedFanOutput.Value = speedFanOutputF1.Value;
                    phaseDuration = durationF1.Value;
                    phaseSetPoint = spF1.Value;
                    currentSetpoint = phaseSetPoint;
                    actualSetPoint.Value = currentSetpoint;
                    actualDurationF1.Value = 0;
                    break;
                case 2:
                    kWh.Value = (kW.Value * durationF1.Value) / 3600.0;
                    speedFanInput.Value = speedFanInputF2.Value;
                    speedFanOutput.Value = speedFanOutputF2.Value;
                    phaseDuration = durationF2.Value;
                    phaseSetPoint = spF2.Value;
                    currentSetpoint = phaseSetPoint;
                    actualSetPoint.Value = currentSetpoint;
                    actualDurationF2.Value = 0;
                    break;
                case 3:
                    kWh.Value = kWh.Value + (kW.Value * durationF2.Value) / 3600.0;
                    speedFanInput.Value = speedFanInputF3.Value;
                    speedFanOutput.Value = speedFanOutputF3.Value;
                    phaseDuration = durationF3.Value;
                    phaseSetPoint = spF3.Value;
                    currentSetpoint = phaseSetPoint;
                    actualSetPoint.Value = currentSetpoint;
                    actualDurationF3.Value = 0;
                    break;
                default:
                    break;
            }
            istantkW = 3.0 * (speedFanInput.Value / 100.0) + 3.0 * (speedFanOutput.Value / 100.0);
            kW.Value = istantkW;
            timeToFinishPhase.Value = phaseDuration;
            timeToFinishAll.Value = allDuration;
        }
        else
        {
            jobsCounter.Value = jobsCounter.Value + 1;
            jobFinish.Value = DateTime.Now;
            jobGas.Value = gas.Value;
            jobkWh.Value = kWh.Value;

            NetLogicObject pushAgent = Project.Current.Find<NetLogicObject>("PushAgent");
            pushAgent.ExecuteMethod("PushNewJob");
            StopOven();
        }
    }

    private void OvenLogic()
    {
        if (ovenRunning)
        {
            ovenLogicCycles++;
            if (phaseDuration <= 0)
            {
                switch ((int)actualPhase.Value)
                {
                    case 1:
                        actualDurationF1.Value = durationF1.Value;
                        break;
                    case 2:
                        actualDurationF2.Value = durationF2.Value;
                        break;
                    case 3:
                        actualDurationF3.Value = durationF3.Value;
                        break;
                    default:
                        break;
                }
                SetPointPhase(actualPhase.Value + 1);
            }
            else
            {
                phaseDuration = phaseDuration - 0.1;
                allDuration = allDuration - 0.1;

            }
            if (ovenLogicCycles >= 10)
            {
                //Random Alarms
                if (temperatureAlarm.Value)
                    temperatureAlarm.Value = false;
                if (fanAlarm.Value)
                    fanAlarm.Value = false;
                if (doorAlarm.Value)
                    doorAlarm.Value = false;
                int nAlarm = rnd.Next(1, 40);
                //Log.Info(nAlarm.ToString());
                if (nAlarm == 1)
                    temperatureAlarm.Value = true;
                nAlarm = rnd.Next(1, 40);
                if (nAlarm == 1)
                    doorAlarm.Value = true;
                nAlarm = rnd.Next(1, 40);
                if (nAlarm == 1)
                    fanAlarm.Value = true;
                //End Random Alarms
                if (useMinuteCounter < filterLimit.Value)
                    useMinuteCounter++;
                ovenLogicCycles = 0;
                timeToFinishPhase.Value = phaseDuration;
                timeToFinishAll.Value = allDuration;
                actualDuration.Value = (int)durationF1.Value + (int)durationF2.Value + (int)durationF3.Value - (int)allDuration;
                switch ((int)actualPhase.Value)
                {
                    case 1:
                        actualDurationF1.Value = durationF1.Value - phaseDuration;
                        break;
                    case 2:
                        actualDurationF2.Value = durationF2.Value - phaseDuration;
                        break;
                    case 3:
                        actualDurationF3.Value = durationF3.Value - phaseDuration;
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void StartOven()
    {
        ResetOven();
        jobStart.Value = DateTime.Now;
        jobRecipe.Value = Project.Current.GetVariable("Model/ProcessData/RecipeName").Value;
        timeToFinishAll.Value = (int)durationF1.Value + (int)durationF2.Value + (int)durationF3.Value;
        allDuration = timeToFinishAll.Value;
        running.Value = true;
        ovenRunning = true;
        SetPointPhase(1);
        ovenLogicCycles = 0;
        periodicTask = new PeriodicTask(OvenLogic, 100, LogicObject);
        periodicTask.Start();
    }

    private void UpdateOvenTemperature()
    {
        temperatureUpdateCounter++;

        if (ovenRunning)
        {
            Calculate_EE_Gas();
            //throttle logic
            if (currentTemperature >= currentSetpoint)
                throttle = 0.0;
            else if (currentSetpoint - currentTemperature > 40)
                throttle = 10.0;
            else
                throttle = 10.0 - (1 - (currentSetpoint - currentTemperature) / 40) * 10;

            if (currentTemperature != currentSetpoint)
            {
                var delta = Math.Abs(currentSetpoint - currentTemperature) * 0.1f;
                if (currentSetpoint >= currentTemperature)
                    currentTemperature += delta;
                else
                    currentTemperature -= delta;
            }
        }
        else if (preheat)
        {
            //throttle logic
            if (currentTemperature >= preheatTemp)
                throttle = 0.0;
            else if (preheatTemp - currentTemperature > 40)
                throttle = 10.0;
            else
                throttle = 10.0 - (1 - (preheatTemp - currentTemperature) / 40) * 10;

            if (currentTemperature != preheatTemp)
            {
                var delta = Math.Abs(preheatTemp - currentTemperature) * 0.1f;
                if (preheatTemp >= currentTemperature)
                    currentTemperature += delta;
                else
                    currentTemperature -= delta;
            }
        }
        else
        {
            if (currentTemperature != 0)
            {
                var delta = Math.Abs(0 - currentTemperature) * 0.1f;
                if (0 >= currentTemperature)
                    currentTemperature += delta;
                else
                    currentTemperature -= delta;
            }
        }

        if (temperatureUpdateCounter > 4)
        {
            if (ovenRunning)
            {
                gas.Value = temp_gas;
                kWh.Value = temp_kwh;
                totEE.Value = (double)totEE.Value + kWh.Value;
                totLPG.Value = (double)totLPG.Value + gas.Value;
            }
            temperatureUpdateCounter = 0;
            gasThrottle.Value = throttle;
            preheat = preheating.Value;
            actualTemperature.Value = currentTemperature;
            useMinute.Value = useMinuteCounter;
        }
    }

    private void Calculate_EE_Gas()
    {
        temp_kwh = temp_kwh + (istantkW * 0.250) / 3600.0;
        temp_gas = temp_gas + ((throttle * 0.250) / 3600.0)/4;
    }

    [ExportMethod]
    public void ResetFilter()
    {
        useMinuteCounter = 0;
    }

}
