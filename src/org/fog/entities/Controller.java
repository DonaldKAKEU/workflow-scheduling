package org.fog.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.power.PowerHost;
import org.fog.offloading.OffloadingStrategy;
import org.fog.utils.Config;
import org.fog.utils.FogEvents;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.NetworkUsageMonitor;
import org.workflowsim.CondorVM;
import org.workflowsim.FileItem;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.utils.Parameters.FileType;

/**
 * Controller contains the evaluating indicators library, which can calculate each indicator.
 * 
 * @since FogWorkflowSim Toolkit 1.0
 * @author Lingmin Fan
 */
public class Controller extends SimEntity{
	
	private List<FogDevice> fogDevices;
	
	/**
     * The workflow engine associated with it.
     */
	private WorkflowEngine wfEngine;
	
	/**
     * The offloading engine associated with it.
     */
	private OffloadingEngine offloadingEngine;
	
	/**
	 * Current time
	 */
	public double TotalExecutionTime;
	
	/**
	 * The Energy of mobile
	 */
	public double TotalEnergy;
	
	/**
	 * The cost of the usage of fog servers and cloud
	 */
	public double TotalCost;
	
	/**
	 * The time of mobile sending data
	 */
	public double MSendTime;//移动设备发送数据时间
	
	/**
	 * The time of mobile receiving data
	 */
	public double MReceTime;//移动设备接收数据时间
	
	double LAN_Bandwidth = 100;//Mbps
	double WAN_Bandwidth = 40;//Mbps
	final double parameter = 10000;//计算传输数据的传输时间的调整参数
	int count1=0, count2=0, count3=0;

    // Custom metrics to be calculated at the end of simulation
    public double customTotalExecutionTime; // Makespan
    public double customTotalCost;          // Total Cost
    public double customTotalEnergy;        // Total Energy

    // C_transfer: Cost of transfer per unit of data/time (Assumed value, adjust as needed)
    private static final double C_TRANSFER = 0.05; 
    
    // Average Bandwidth assumption if link specific not available (Mbps -> converted to bits/sec in logic)
    private double AVERAGE_BW = 100.0;
	
	public Controller(String name, List<FogDevice> fogDevices , WorkflowEngine Engine) {
		super(name);
		for(FogDevice fogDevice : fogDevices){
			fogDevice.setControllerId(getId());
		}
		setFogDevices(fogDevices);
		wfEngine = Engine;
		wfEngine.setcontrollerId(this.getId());

		offloadingEngine = wfEngine.getoffloadingEngine();
		offloadingEngine.setfogDevices(fogDevices);

		TotalExecutionTime=0.0;
		TotalEnergy=0.0;
		TotalCost=0.0;
		MSendTime=0.0;
	}

	public FogDevice getFogDeviceById(int id){
		for(FogDevice fogDevice : getFogDevices()){
			if(id==fogDevice.getId())
				return fogDevice;
		}
		return null;
	}
	
    @Override
    public void startEntity() {}

	@Override
	public void processEvent(SimEvent ev) {
		switch(ev.getTag()){
		case FogEvents.CONTROLLER_RESOURCE_MANAGE:
			manageResources();
			break;
		case FogEvents.STOP_SIMULATION:
			shutdownEntity();
			CloudSim.clearEvent();
			updateExecutionResults();
			CloudSim.stopSimulation();
			break;
		}
	}

    public void updateExecutionResults() {
        // 1. Makespan = max(ET_i)
        customTotalExecutionTime = CloudSim.clock();

        // 2. Total Cost = Processing Cost + Communication Cost
        customTotalCost = calculateTotalCost();

        // 3. Total Energy = Active Energy + Idle Energy (for all devices)
        customTotalEnergy = calculateTotalEnergy();
    }

    /**
     * Calculates Cost based on:
     * Cost = sum(CT_i * C^proc_j) + sum(Comm_Cost)
     */
    private double calculateTotalCost() {
        double processingCost = 0.0;
        double communicationCost = 0.0;
        
        List<Job> jobList = wfEngine.getJobsReceivedList();

        for (Job job : jobList) {
            CondorVM vm = getVm(job.getVmId());
            if (vm == null) continue;

            // --- A. Processing Cost ---
            // Formula: CT_i * C^proc_j
            // CT_i = Actual CPU Time
            // C^proc_j = Cost per Second of the VM (derived from Host/Datacenter characteristics)
            double CT_i = job.getActualCPUTime();
            double C_proc_j = vm.getHost().getDatacenter().getCharacteristics().getCostPerSecond();
            
            processingCost += (CT_i * C_proc_j);

            // --- B. Communication Cost ---
            // Formula: If diff VM: (Dout / BW) * C_transfer
            // We look at the job's parents (Predecessors) to find data transfer
            List<Task> parents = job.getParentList();
            for (Task parent : parents) {
                Job parentJob = (Job) parent; // Cast Task to Job
                
                // If on different VMs (or one is on mobile, one on cloud, etc.)
                if (parentJob.getVmId() != job.getVmId()) {
                    
                    // Volume of data (D_out of parent -> D_in of current)
                    // We sum up files that are OUTPUT in parent and INPUT in child
                    double dataSizeKbits = getTransferDataSize(parentJob, job) * 8 / 1024; // Convert bytes to kbits
                    
                    // Bandwidth (Mbps)
                    // Simplified: Using average BW or retrieving from VM
                    double bwMbps = vm.getBw() / 1000.0; // Assuming getBw is in Kbps, convert to Mbps
                    if(bwMbps <= 0) bwMbps = AVERAGE_BW;

                    // Transfer Time
                    double transferTime = dataSizeKbits / (bwMbps * 1024); // (kbits / kbps) = seconds
                    
                    // Cost accumulation
                    communicationCost += (transferTime * C_TRANSFER);
                }
            }
        }
        
        return processingCost + communicationCost;
    }

    /**
     * Calculates Energy based on:
     * Energy = P_active * ActiveTime + P_idle * IdleTime
     */
    private double calculateTotalEnergy() {
        double totalEnergy = 0.0;

        for (FogDevice device : getFogDevices()) {
            PowerHost host = (PowerHost) device.getHost();
            FogLinearPowerModel powerModel = (FogLinearPowerModel) host.getPowerModel();

            // P_idle and P_active (max)
            double P_idle = powerModel.getStaticPower(); 
            double P_active = powerModel.getMaxPower(); 

            // Calculate Active Time (Sum of execution time of all tasks on this device)
            double activeTime = 0.0;
            List<Job> jobList = wfEngine.getJobsReceivedList();
            for(Job job : jobList){
                if (getVm(job.getVmId()).getHost().getId() == host.getId()) {
                    activeTime += job.getActualCPUTime();
                }
            }

            // Idle Time = Total Makespan - Active Time
            double idleTime = TotalExecutionTime - activeTime;
            if(idleTime < 0) idleTime = 0;

            // Energy for this device
            // Note: The model says Energy = P_active * Time (during execution). 
            // Strictly speaking, P_active varies by load. 
            // Simplified here: Energy = (P_active * activeTime) + (P_idle * idleTime)
            double calculatedEnergy = (P_active * activeTime) + (P_idle * idleTime);

            
            // Apply to total
            totalEnergy += calculatedEnergy;
            
            // Update device record for reporting
            device.setEnergyConsumption(calculatedEnergy);
        }
        return totalEnergy;
    }

    /**
     * Helper to calculate data size transferred between Parent and Child
     */
    private double getTransferDataSize(Job parent, Job child) {
        double size = 0.0;
        // Logic: Find files that are Output of Parent and Input of Child
        for (FileItem pFile : parent.getFileList()) {
            if (pFile.getType() == FileType.OUTPUT) {
                for (FileItem cFile : child.getFileList()) {
                    if (cFile.getType() == FileType.INPUT && cFile.getName().equals(pFile.getName())) {
                        size += cFile.getSize();
                    }
                }
            }
        }
        return size;
    }
	
	private void printNetworkUsageDetails() {
		System.out.println("Total network usage = "+NetworkUsageMonitor.getNetworkUsage()/Config.MAX_SIMULATION_TIME);		
	}

	private void printCostDetails(){
		System.out.println("Total Cost = "+TotalCost);
	}
	
	public void updateExecutionTime() {
		double time = 0.0;
		double energy = 0.0;
		double cost = 0.0;
		double WAN_sendInput = 0, WAN_sendOutput = 0;
		double LAN_sendInput = 0, LAN_sendOutput = 0;
		
		getmobile().setEnergyConsumption(getMobileEnergy());
		List<Job> jobList = wfEngine.getJobsReceivedList();
		for(Job job : jobList){
			if(getDC(job.getVmId()) == getcloud().getId()){
				count1++;
				WAN_sendInput += job.getInputsize();
//				WAN_sendOutput += job.getOutputsize();
			}
			else if(getDC(job.getVmId()) == getFogNode().getId()){
				count2++;
				LAN_sendInput += job.getInputsize();
//				LAN_sendOutput += job.getOutputsize();
			}
			else if(getDC(job.getVmId()) == getmobile().getId()){
				count3++;
			}
		}
		
		Job Lastjob = jobList.get(jobList.size()-1);//获取到最后一个执行的job
		for(FileItem file : Lastjob.getFileList()){
			if(file.getType() == FileType.OUTPUT)
				if(getDC(Lastjob.getVmId()) == getcloud().getId())
					WAN_sendOutput += Lastjob.getOutputsize();
				else if(getDC(Lastjob.getVmId()) == getFogNode().getId())
					LAN_sendOutput += Lastjob.getOutputsize();
		}
		
		for(FogDevice fogDevice : getFogDevices())
		{
			String name=fogDevice.getName();
			time += fogDevice.getExecutionTime();
			
			if(name.contains("m")){
//				List<Job> jobList = wfEngine.getJobsReceivedList();
//				if(!jobList.isEmpty()){
//					Job Lastjob = jobList.get(jobList.size()-1);//获取到最后一个执行的job
//					for(FileItem file : Lastjob.getFileList()){
//						if(file.getType() == FileType.OUTPUT)
//							MReceTime += (file.getSize()/1024) / getCloud().getDownlinkBandwidth();
//						//文件大小单位为bit,换算成Kb,带宽单位为Kbps,接收时间单位为s
//					}
//				}
//				MSendTime = (getSendSize()/1024) / getmobile().getUplinkBandwidth();
				MSendTime = LAN_sendInput / parameter / LAN_Bandwidth + WAN_sendInput / parameter / WAN_Bandwidth;
				MReceTime = LAN_sendOutput / parameter / LAN_Bandwidth + WAN_sendOutput / parameter / WAN_Bandwidth;
				FogLinearPowerModel powerModel = (FogLinearPowerModel) fogDevice.getHost().getPowerModel();
				//移动设备能耗  = (空闲能耗 +负载能耗) +发送数据能耗+接收数据能耗
				energy = fogDevice.getEnergyConsumption() + MSendTime * powerModel.getSendPower() +
				               MReceTime * powerModel.getRecePower();
				fogDevice.setEnergyConsumption(energy/1000);//单位J
			}
			else{
				fogDevice.setTotalCost(getDatacenterCost(fogDevice.getId()));
				cost += getDatacenterCost(fogDevice.getId());
			}
		}
		TotalExecutionTime = CloudSim.clock();
		TotalEnergy = getmobile().getEnergyConsumption();
		TotalCost = cost;
	}

	private void printPowerDetails() {
		FogDevice mobile = getmobile();
		System.out.println(mobile.getName() +" : Energy Consumed = "+mobile.getEnergyConsumption() +" J");
	}

	protected void manageResources(){
		for(FogDevice dev : getFogDevices())
			sendNow(dev.getId(), FogEvents.RESOURCE_MGMT);
	}
	
	@Override
	public void shutdownEntity() {	
		Log.printLine(getName() + " is shutting down...");
	}

	public List<FogDevice> getFogDevices() {
		return fogDevices;
	}

	public void setFogDevices(List<FogDevice> fogDevices) {
		this.fogDevices = fogDevices;
	}
	
	public void print()
	{
		// Log.printLine();
		// Log.printLine("=========================================================");
		// System.out.println("Algorithm is running "+wfEngine.algorithmTime+"ms");
		// System.out.println("Workflow Makespan = "+TotalExecutionTime);
		// printPowerDetails();
		// printCostDetails();
		// System.out.println("Offloading to Cloud: "+count1+", to Fog: "+count2+", and in mobile: "+count3+".");
		// //printNetworkUsageDetails();
		// Log.printLine("=========================================================");
		// Log.printLine();

        Log.printLine();
        Log.printLine("v2=========================================================");
        System.out.println("Algorithm is running id: " + wfEngine.algorithmTime );
        System.out.println("Workflow Makespan = " + customTotalExecutionTime);
        System.out.println("Total Energy      = " + customTotalEnergy + " J");
        System.out.println("Total Cost        = " + customTotalCost + " Euro");
        
        Log.printLine();
	}
	public void clear()
	{
		for(FogDevice device: fogDevices)
			device.clearConsumption();
		MSendTime = 0.0;
		MReceTime = 0.0;
		count1=0; count2=0; count3=0;
	}
	
	public double getSendSize(){
		double sendsize=0;
		List<Job> jobList=wfEngine.getJobsReceivedList();
		HashMap<String, List<Integer>> fileToDatacenter= new HashMap<String, List<Integer>>();
		
		for(Job job:jobList){
			CondorVM vm = getVm(job.getVmId());
			int datacenterId = vm.getHost().getDatacenter().getId();
			if(!CloudSim.getEntity(datacenterId).getName().contains("m")){
				for(FileItem file : job.getFileList()){
					List<Integer> datacenterIdList = new ArrayList<Integer>();
					if(file.getType() == FileType.INPUT){
						if(!fileToDatacenter.containsKey(file.getName())){
							sendsize += file.getSize();
						}
						else if(fileToDatacenter.containsKey(file.getName()) && !fileToDatacenter.get(file.getName()).contains(datacenterId)){
							datacenterIdList = fileToDatacenter.get(file.getName());
						}
						datacenterIdList.add(datacenterId);
						fileToDatacenter.put(file.getName(), datacenterIdList);
					}
				}
			}
		}
		return sendsize;
	}
	
	public CondorVM getVm(int vmId){
		for(Vm vm:wfEngine.getAllVmList()){
			if(vm.getId()==vmId)
				return (CondorVM) vm;
		}
		return null;
	}
	
	public int getDC(int vmId){
		for(Vm vm:wfEngine.getAllVmList()){
			if(vm.getId()==vmId)
				return vm.getHost().getDatacenter().getId();
		}
		return 0;
	}
	
	public double getDatacenterCost(int id) {
		double cost = 0;
		List<Job> jobList = wfEngine.getJobsReceivedList();
		for(Job job : jobList){
			CondorVM vm = getVm(job.getVmId());
			PowerHost host = (PowerHost) vm.getHost();
			FogDevice fogdevice = (FogDevice) host.getDatacenter();
			int datacenterId = fogdevice.getId();
			if(!CloudSim.getEntity(datacenterId).getName().contains("m")){
				if(id == datacenterId) {
					//cost+=job.getProcessingCost();
					double c = job.getActualCPUTime()* vm.getHost().getTotalMips()/vm.getMips() * host.getcostPerMips();
					//费用 = 执行时间 * 计算资源使用量 * 每单位计算量每单位时间的计算费用
					job.setProcessingCost(c);
					cost += c;
				}
			}
			else {
				job.setProcessingCost(0);
			}
		}
		return cost;
	}
	
	public double getMobileEnergy(){
		double energy = 0;
		double executiontime = 0;
		double idletime = 0;
		List<Job> jobList=wfEngine.getJobsReceivedList();
		for(Job job:jobList){
			CondorVM vm = getVm(job.getVmId());
			PowerHost host = (PowerHost)vm.getHost();
			FogLinearPowerModel powerModel = (FogLinearPowerModel) host.getPowerModel();
			FogDevice fogdevice = (FogDevice) vm.getHost().getDatacenter();
			if(fogdevice.getName().contains("m")){
				executiontime += job.getActualCPUTime();
				double  e = job.getActualCPUTime() * powerModel.getPower(vm.getMips()/host.getTotalMips());
				energy += e;
			}
		}
		idletime = CloudSim.clock()-executiontime;
		FogLinearPowerModel powerModel = (FogLinearPowerModel) getmobile().getHost().getPowerModel();
		energy += idletime * powerModel.getStaticPower();//负载能耗+空闲能耗
		return energy;
	}
	
	public FogDevice getmobile(){
		for(FogDevice dev : getFogDevices())
			if(dev.getName().contains("m"))
				return dev;
		return null;
	}
	
	public FogDevice getcloud(){
		for(FogDevice dev : getFogDevices())
			if(dev.getName().equalsIgnoreCase("cloud"))
				return dev;
		return null;
	}
	
	public FogDevice getFogNode(){
		for(FogDevice dev : getFogDevices())
			if(dev.getName().contains("f"))
				return dev;
		return null;
	}
}