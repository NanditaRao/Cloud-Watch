import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.*;
import java.util.*;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.storagegateway.model.ListVolumesRequest;


public class ScalingOfInstances {

	private String userID;
	private AmazonEC2 ec2;
	private AmazonCloudWatchClient cwc;
	private boolean initialised;
	private Map<String, InstanceInfo> instanceIDs;
	private String securityGroup;
	private String keyPair;
	private String imageName;
	
	
	public String getUserID() {
		return userID;
	}

	private ScheduledThreadPoolExecutor monitor;
	private ExecutorService creator;
	private ExecutorService destroy;

	public ScalingOfInstances(String userID, AmazonEC2 ec2,
			AmazonCloudWatchClient cwc) {
		this.userID = userID;
		this.ec2 = ec2;
		this.cwc = cwc;
		initialised = false;
		instanceIDs = Collections
				.synchronizedMap(new HashMap<String, InstanceInfo>());
		startCreating(false);
		startMonitoring();
		startDestroying();
	}

	public Map<String, InstanceInfo> getInstanceIDs() {
		return instanceIDs;
	}

	private void startMonitoring() {
		monitor = new ScheduledThreadPoolExecutor(1);
		monitor.scheduleAtFixedRate(new MonitorTask(), 3, 2,
				TimeUnit.MINUTES);
	}

	private void startCreating(boolean force) {
		creator = Executors.newFixedThreadPool(1);
		if (force) {
			creator.execute(new CreateTask());
		} else {
			Calendar calendar = Calendar.getInstance();

			while (calendar.get(Calendar.HOUR_OF_DAY) <= -1) {
				try {
					System.out.println("Waiting");
					Thread.sleep(1000 * 2 * 60);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			creator.execute(new CreateTask());
		}
	}

	private void startDestroying() {
		destroy = Executors.newFixedThreadPool(1);
	}

	private class CreateTask implements Runnable {

		@Override
		public void run() {
			System.out.println("Create Task");
			if (!initialised) {
				createNewInstance(true);
			} else {
				createNewInstance(false);
			}
		}
	}

	private class MonitorTask implements Runnable {

		@Override
		public void run() {

			try {
				System.out.println("Monitor Task");

				Set<Entry<String, InstanceInfo>> entrySet = new HashSet<Entry<String, InstanceInfo>>(
						getInstanceIDs().entrySet());
				for (Entry<String, InstanceInfo> entry : entrySet) {
					String instanceID = entry.getKey();

					GetMetricStatisticsRequest statisticsRequest = new GetMetricStatisticsRequest()
							.withStatistics("Average").withNamespace("AWS/EC2")
							.withMetricName("CPUUtilization").withPeriod(60);

					// set time
					GregorianCalendar calendar = new GregorianCalendar(
							TimeZone.getTimeZone("UTC"));
					calendar.add(GregorianCalendar.SECOND,
							-1 * calendar.get(GregorianCalendar.SECOND));
					Date endTime = calendar.getTime();
					calendar.add(GregorianCalendar.MINUTE, -10);

					Date startTime = calendar.getTime();
					statisticsRequest.setStartTime(startTime);
					statisticsRequest.setEndTime(endTime);

					// specify an instance
					ArrayList<Dimension> dimensions = new ArrayList<Dimension>();
					dimensions.add(new Dimension().withName("InstanceId")
							.withValue(instanceID));
					statisticsRequest.setDimensions(dimensions);

					// get statistics
					GetMetricStatisticsResult statisticsResult = cwc
							.getMetricStatistics(statisticsRequest);

					try {
						System.out.println("Waiting for it to be ready 2");
						Thread.sleep(1000 * 2 * 60);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					List<Datapoint> dataList = statisticsResult.getDatapoints();
					System.out.println("stats = " + dataList.size());

					/*
					 * if (dataList.size() == 0) { Datapoint pt = new
					 * Datapoint();
					 * 
					 * pt.setAverage(10.0);
					 * System.out.println("Added dummy 0.10");
					 * 
					 * dataList.add(pt); }
					 */

					for (Datapoint dataPoint : dataList) {
						Double averageCPU = dataPoint.getAverage();
						System.out.println("CPU Utilization for " + instanceID
								+ " = " + averageCPU);
						if (averageCPU < 20) {
							destroy.execute(new DestroyTask(instanceID));
						} else if (averageCPU > 75) {
							creator.execute(new CreateTask());
						}
					}

					System.out.println("Exiting without error");
				}
			} catch (AmazonServiceException e) {
				e.printStackTrace();
			} catch (AmazonClientException e) {
				e.printStackTrace();
			}
		}
	}

	private class DestroyTask implements Runnable {

		private String instanceID;

		public DestroyTask(String instanceID) {
			this.instanceID = instanceID;
		}

		@Override
		public void run() {
			try {
				System.out.println("DestroyTask");
				if (getInstanceIDs().containsKey(instanceID)) {

					List<String> volumes = new ArrayList<String>(
							getInstanceIDs().get(instanceID).getVolumeIDs());
					// Detach Volumes
					for (String volumeID : volumes) {
						detatchVolume(instanceID, volumeID);
					}
					detachIP(instanceID);
					createAndUpdateImage(instanceID);
					TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
							.withInstanceIds(instanceID);
					ec2.terminateInstances(terminateInstancesRequest);
					getInstanceIDs().remove(instanceID);

				}
			} catch (AmazonServiceException e) {
				e.printStackTrace();
			} catch (AmazonClientException e) {
				e.printStackTrace();
			}

		}

	}

	private class InstanceInfo {
		private String zoneRegion;
		private String IP;
		private List<String> volumeIDs;

		public List<String> getVolumeIDs() {
			return volumeIDs;
		}

		public InstanceInfo(String zoneRegion, String IP) {
			this.zoneRegion = zoneRegion;
			this.IP = IP;
			volumeIDs = new ArrayList<String>();
		}

		public String getZoneRegion() {
			return zoneRegion;
		}

		public String getIP() {
			return IP;
		}

		public void addVolume(String volumeID) {
			volumeIDs.add(volumeID);
		}

		public void removeVolume(String volumeID) {
			volumeIDs.remove(volumeID);
		}
	}

	private void createNewInstance(boolean initial) {

		try {
			System.out.println("createNewInstance");
			if (initial) {
				createKeyPair();
				createSecurityGroup();
			}
			String newInstanceID = actuallyCreateInstance(initial);
			// allocateAndAssociateIP(newInstanceID, initial);
			createAndAttachS3Volume(newInstanceID);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void createAndUpdateImage(String instanceID) {
		System.out.println("createAndUpdateImage");
		CreateImageRequest createImageRequest = new CreateImageRequest();
		createImageRequest.setInstanceId(instanceID);
		createImageRequest.setName("Image_" + instanceID);

		CreateImageResult createImageResult = ec2
				.createImage(createImageRequest);
		imageName = createImageResult.getImageId();
		

	}

	private String actuallyCreateInstance(boolean initial) {
		System.out.println("actuallyCreateInstance");
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
				.withImageId(getImageName(initial)).withMaxCount(1)
				.withMinCount(1).withInstanceType(getInstanceType())
				.withKeyName(keyPair).withSecurityGroups(securityGroup);

		RunInstancesResult result = ec2.runInstances(runInstancesRequest);
		List<Instance> resultInstance = result.getReservation().getInstances();
		String createdInstanceId = null;
		String zone = null;
		for (Instance instance : resultInstance) {
			createdInstanceId = instance.getInstanceId();
			zone = instance.getPlacement().getAvailabilityZone();
		}

		List<String> resources = new LinkedList<String>();
		resources.add(createdInstanceId);
		List<Tag> tags = new LinkedList<Tag>();
		tags.add(new Tag("Name", "Instance" + getInstanceIDs().size()));
		CreateTagsRequest ctr = new CreateTagsRequest(resources, tags);
		ec2.createTags(ctr);

		try {
			System.out.println("Waiting for it to be ready");
			Thread.sleep(1000 * 2 * 60);
		} catch (InterruptedException e) {

		}

		String publicIp = allocateAndAssociateIP(createdInstanceId, initial);

		getInstanceIDs().put(createdInstanceId,
				new InstanceInfo(zone, publicIp));

		
		return createdInstanceId;

	}

	private String getInstanceType() {
		return "t1.micro";
	}

	private String getImageName(boolean initial) {
		if (initial)
			return "ami-aecd60c7";
		else
			return imageName;
	}

	private void createSecurityGroup() {
		System.out.println("createSecurityGroup");

		// Create Security Group
		CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest();
		securityGroup = "SecurityGroup_" + ((int) (Math.random() * 1000));
		securityGroupRequest.setGroupName(securityGroup);
		securityGroupRequest.setDescription(securityGroup);

		boolean exists = false;
		DescribeSecurityGroupsResult securityGroupsResult = ec2
				.describeSecurityGroups();
		List<SecurityGroup> securityGroups = securityGroupsResult
				.getSecurityGroups();
		for (SecurityGroup securityGroup : securityGroups) {
			if (securityGroup.getGroupName().equals(securityGroup)) {
				exists = true;
			}
		}

		if (!exists) {
			@SuppressWarnings("unused")
			CreateSecurityGroupResult securityGroupResult = ec2
					.createSecurityGroup(securityGroupRequest);

			ArrayList<String> IpRanges = new ArrayList<String>();
			IpRanges.add("0.0.0.0/0");
			ArrayList<IpPermission> ipPermissions = new ArrayList<IpPermission>();
			ipPermissions.add(new IpPermission().withIpProtocol("tcp")
					.withFromPort(22).withToPort(22).withIpRanges(IpRanges));
			ipPermissions.add(new IpPermission().withIpProtocol("tcp")
					.withFromPort(80).withToPort(80).withIpRanges(IpRanges));
			ipPermissions.add(new IpPermission().withIpProtocol("tcp")
					.withFromPort(443).withToPort(443).withIpRanges(IpRanges));

			AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest = new AuthorizeSecurityGroupIngressRequest(
					securityGroup, ipPermissions);
			ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
		}
		
	}

	private void createKeyPair() {
		// Create Key Pair
		System.out.println("createKeyPair");
		CreateKeyPairRequest keyPairRequest = new CreateKeyPairRequest();
		keyPair = "KeyPair_" + userID;
		keyPairRequest.setKeyName(keyPair);

		boolean exists = false;
		DescribeKeyPairsResult describeKeyPairsResult = ec2.describeKeyPairs();
		List<KeyPairInfo> keyPairs = describeKeyPairsResult.getKeyPairs();
		for (KeyPairInfo keyPairInfo : keyPairs) {
			if (keyPairInfo.getKeyName().equals(keyPair)) {
				exists = true;
			}
		}

		if (!exists) {
			CreateKeyPairResult keyPairResult = ec2
					.createKeyPair(keyPairRequest);
			try {
				BufferedWriter out = new BufferedWriter(new FileWriter(
						getKeyFileName()));
				out.write(keyPairResult.getKeyPair().getKeyMaterial());
				out.close();
			} catch (IOException e) {

			}
		}
		
	}

	private String getKeyFileName() {
		return "/home/" + userID + "/" + keyPair + ".pem";
	}

	private String allocateAndAssociateIP(String newInstanceID, boolean initial) {
		System.out.println("allocateAndAssociateIP");
		// allocate
		// if (initial) {
		AllocateAddressResult elasticResult = ec2.allocateAddress();
		String publicIp = elasticResult.getPublicIp();
		// }

		System.out.println("AllocatedIP  = " + publicIp + " Instance ID = "
				+ newInstanceID);

		AssociateAddressRequest associateAddressRequest = new AssociateAddressRequest();
		associateAddressRequest.setInstanceId(newInstanceID);
		associateAddressRequest.setPublicIp(publicIp);
		ec2.associateAddress(associateAddressRequest);

		
		return publicIp;
	}

	public void detachIP(String instanceID) {
		System.out.println("detachIP");
		// dissociate
		DisassociateAddressRequest disassociateAddressRequest = new DisassociateAddressRequest();
		disassociateAddressRequest.setPublicIp(getInstanceIDs().get(instanceID)
				.getIP());
		ec2.disassociateAddress(disassociateAddressRequest);
		
	}

	public void createAndAttachS3Volume(String instanceID) throws IOException {
		System.out.println("createAndAttachS3Volume");
		// create a volume
		
		
		String volumeID = ""; 
		// else write
		
		BufferedReader reader1 = new BufferedReader(new FileReader("C:\\UserVolume1.txt"));
		BufferedReader reader2 = new BufferedReader(new FileReader("C:\\UserVolume2.txt"));
		
		PrintWriter out1 = new PrintWriter(new BufferedWriter(new FileWriter("C:\\UserVolume1.txt", true)));
		PrintWriter out2 = new PrintWriter(new BufferedWriter(new FileWriter("C:\\UserVolume2.txt", true)));
		
	    String line1, line2;
	    boolean found1 = false;
	    boolean found2 = false;
	    
	    if(this.userID.equals("User1"))
	    {

	    	line1 = reader1.readLine();
	    	System.out.println("Reading file....."+line1);
	    	
	    	if(line1 != null)
	    	{
	    		String[] userVolumeInfo = new String[2]	;	
	    		
	    		userVolumeInfo = line1.split(" ");
	    		System.out.println(userVolumeInfo[0]+" read from the file ");
	    		
	    			volumeID = userVolumeInfo[1].trim();
	    			System.out.println("Volume id "+this.userID+" "+volumeID);
	    			found1 = true;

	    		
	    	}



	    	if(!found1)
	    	{
	    		CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest();
	    		createVolumeRequest.setAvailabilityZone(getInstanceIDs()
	    				.get(instanceID).getZoneRegion());
	    		createVolumeRequest.setSize(1); // size = 1 GB
	    		CreateVolumeResult createVolumeResult = ec2
	    				.createVolume(createVolumeRequest);
	    		System.out.println("Created ID");
	    		volumeID = createVolumeResult.getVolume().getVolumeId();
	    		System.out.println("Volume id "+this.userID+" "+volumeID);
	    		out1.println(this.userID+" "+volumeID+"\n");
	    		out1.close();
	    		
	    	}
	    }

	    if(this.userID.equals("User2"))
	    {
	    	line2 = reader2.readLine();
	    	System.out.println("Reading file....."+line2);
	    	{
	    		System.out.println("Reading file.....");
	    		if(line2 != null)
	    		{
	    			String[] userVolumeInfo = new String[2];
	    			
	    			userVolumeInfo = line2.split(" ");
	    			if(userVolumeInfo[0].equals(this.userID))
	    			{
	    				volumeID = userVolumeInfo[1].trim();
	    				System.out.println("Volume id "+this.userID+" "+volumeID);
	    				found2 = true;

	    			}

	    		}


	    		if(!found2)
	    		{
	    			CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest();
	    			createVolumeRequest.setAvailabilityZone(getInstanceIDs()
	    					.get(instanceID).getZoneRegion());
	    			createVolumeRequest.setSize(1); // size = 1 GB
	    			CreateVolumeResult createVolumeResult = ec2
	    					.createVolume(createVolumeRequest);
	    			System.out.println("Created ID");
	    			volumeID = createVolumeResult.getVolume().getVolumeId();
	    			System.out.println("Volume id "+this.userID+" "+volumeID);
	    			out2.println(this.userID+" "+volumeID+"\n");
	    			out2.close();
	    			
	    		}
	    	}
			
		AttachVolumeRequest attachVolumeRequest = new AttachVolumeRequest();
		attachVolumeRequest.setVolumeId(volumeID);
		attachVolumeRequest.setInstanceId(instanceID);
		attachVolumeRequest.setDevice("/dev/sdi");
		ec2.attachVolume(attachVolumeRequest);

		getInstanceIDs().get(instanceID).addVolume(volumeID);
		
	}
	}

	public void detatchVolume(String instanceID, String volumeID) {
		System.out.println("detatchVolume");
		DetachVolumeRequest detachVolumeRequest = new DetachVolumeRequest();
		detachVolumeRequest.setVolumeId(volumeID);
		detachVolumeRequest.setInstanceId(instanceID);
		getInstanceIDs().get(instanceID).removeVolume(volumeID);
		ec2.detachVolume(detachVolumeRequest);
		
	}

}