import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;

public class MainProgram {

	private static Map<String, ScalingOfInstances> instances = new HashMap<String, ScalingOfInstances>();
	private static AmazonEC2 ec2 = null;
	private static AmazonCloudWatchClient cwc = null;

	public static void main(String args[]) {
		try {
			
						
			createNewElasticInstance("User1");
			createNewElasticInstance("User2");
			
			
		} catch (IOException e) {
			System.out.println("Error!!!!!");
		}

	}

	private static AmazonEC2 getEC2() throws IOException {
		if (ec2 == null) {
			AWSCredentials credentials = new PropertiesCredentials(
					MainProgram.class
							.getResourceAsStream("AwsCredentials.properties"));
			ec2 = new AmazonEC2Client(credentials);
		}

		return ec2;
	}

	private static AmazonCloudWatchClient getCWC() throws IOException {
		if (cwc == null) {
			AWSCredentials credentials = new PropertiesCredentials(
					MainProgram.class
							.getResourceAsStream("AwsCredentials.properties"));
			cwc = new AmazonCloudWatchClient(credentials);
		}

		return cwc;
	}

	private static void createNewElasticInstance(String userID)
			throws IOException {
		ScalingOfInstances instance = new ScalingOfInstances(userID, getEC2(),
				getCWC());
		instances.put(userID, instance);
	}

}
