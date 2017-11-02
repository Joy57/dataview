package builtin;

import implicitshims.ConversionFunction;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.dropbox.core.DbxException;

import toyDPM.JavaDropbox;
import translator.ExecutionStatus;
import translator.Experiment;
import translator.SWLAnalyzer;
import translator.Workflow;
import utility.Utility;
import dataProduct.BooleanDP;
import dataProduct.DataProduct;
import dataProduct.DropboxDP;
import dataProduct.IntegerDP;

/**
 * Builtin workflow
 * @author Andrey Kashlev
 *
 */
public class Multiply extends Workflow {
	public Multiply(String instanceid) {
		this.instanceId = instanceid;
		executionStatus = ExecutionStatus.initialized;
	}

	@Override
	public void execute(String runID, boolean registerIntermediateDPs) throws Exception {
		System.out.println("running workflow " + instanceId);
		if (!readyToExecute())
			return;

		Iterator it = inputPortID_to_DP.values().iterator();
		int a = 0;
		int b = 0;
		String filename1 = null;
		String filename2 = null;
		String firstdpType = null;
		String seconddpType = null;
		String outputdpType = null;
		// get input data mapped to input port?
		HashSet<String> inputMapping1 = new HashSet<String>();
		inputMapping1 = SWLAnalyzer.getInputMappingFromAttribute(Experiment.workflowSpec, this.instanceId + ".i1");
		
		if (inputMapping1 == null || inputMapping1.size() == 0){
			inputMapping1 = SWLAnalyzer.getDataChannelsFromAttribute(Experiment.workflowSpec, this.instanceId , "i1");
		}
		
		HashSet<String> inputMapping2 = new HashSet<String>();
		inputMapping2 = SWLAnalyzer.getInputMappingFromAttribute(Experiment.workflowSpec, this.instanceId + ".i2");
		
		if (inputMapping2 == null || inputMapping2.size() == 0){
			inputMapping2 = SWLAnalyzer.getDataChannelsFromAttribute(Experiment.workflowSpec, this.instanceId , "i2");
		}
		
		for (String mapping : inputMapping1){
			if (mapping.indexOf("_") >= 0)
				mapping = mapping.substring(5, mapping.indexOf("_"));
			firstdpType = Utility.getDPLTypeFromDB(mapping);
		}
		for (String mapping : inputMapping2){
			if (mapping.indexOf("_") >= 0)
				mapping = mapping.substring(5, mapping.indexOf("_"));
			seconddpType = Utility.getDPLTypeFromDB(mapping);
		}
		
		System.out.println(Utility.nodeToString(Experiment.workflowSpec));

		HashSet<String> outputMapping = new HashSet<String>();
		outputMapping = SWLAnalyzer.getOutputMappingToAttribute(Experiment.workflowSpec, this.instanceId + ".o1");
		
		if (outputMapping == null || outputMapping.size() == 0){
			System.out.println("getting data channel information");
			System.out.println(this.instanceId);
			outputMapping = SWLAnalyzer.getDataChannelsToAttribute(Experiment.workflowSpec, this.instanceId , "o1");
		}
		
		for (String mapping : outputMapping){
			System.out.println(mapping);
			outputdpType = mapping.substring(mapping.indexOf("TO_") + 3, mapping.length());
			
		}
		System.out.println(firstdpType);
		System.out.println(seconddpType);
		// get dpl type of the input data?
		if (firstdpType.equalsIgnoreCase("Dropbox")){
			filename1 = ((DropboxDP) inputPortID_to_DP.get("i1")).dropboxPath;
			String key = ((DropboxDP) inputPortID_to_DP.get("i1")).dropboxAppKey;
			String secret = ((DropboxDP) inputPortID_to_DP.get("i1")).dropboxAppSecret;
			String token = ((DropboxDP) inputPortID_to_DP.get("i1")).dropboxAppToken;
			JavaDropbox.initializeDropbox(key, secret, token);
			String dropboxPath = filename1;
			System.out.println(dropboxPath);
			filename1 = filename1.substring(filename1.lastIndexOf("/")+1, filename1.length());
			System.out.println(filename1);
			String downloadedFilePath = Utility.pathToConfigFile.substring(0, Utility.pathToConfigFile.lastIndexOf("/")+1) + filename1;
			JavaDropbox.downloadFromDropbox(dropboxPath, downloadedFilePath);
			System.out.println("#downloaded file from dropbox..");

			a = Integer.parseInt(ConversionFunction.fileToScalar(downloadedFilePath, "integer").trim());
			System.out.println("#converted file into integer.." + a);
			
		}
		else{
			if (inputPortID_to_DP.get("i1") instanceof IntegerDP) {
				a = ((IntegerDP) inputPortID_to_DP.get("i1")).data;
			}
		}
		
		if (seconddpType.equalsIgnoreCase("Dropbox")){
			filename2 = ((DropboxDP) inputPortID_to_DP.get("i2")).dropboxPath;
			String key = ((DropboxDP) inputPortID_to_DP.get("i2")).dropboxAppKey;
			String secret = ((DropboxDP) inputPortID_to_DP.get("i2")).dropboxAppSecret;
			String token = ((DropboxDP) inputPortID_to_DP.get("i2")).dropboxAppToken;
			JavaDropbox.initializeDropbox(key, secret, token);
			String dropboxPath = filename2;
			System.out.println(dropboxPath);
			filename2 = filename2.substring(filename2.lastIndexOf("/")+1, filename2.length());
			System.out.println(filename2);
			
			String downloadedFilePath = Utility.pathToConfigFile.substring(0, Utility.pathToConfigFile.lastIndexOf("/")+1) + filename2;
			JavaDropbox.downloadFromDropbox(dropboxPath, downloadedFilePath);
			System.out.println("#downloaded file from dropbox..");
			b = Integer.parseInt(ConversionFunction.fileToScalar(downloadedFilePath, "integer").trim());
			System.out.println("#converted file into scalar integer..."+ b);
		
		}
		else{
			if (inputPortID_to_DP.get("i2") instanceof IntegerDP)
				b = ((IntegerDP) inputPortID_to_DP.get("i2")).data;
		}
		executionStatus = ExecutionStatus.inProcessOfExecution;
		int c = 0;
		String resultFileName = instanceId + ".o1." + runID;
		try {
			c = a * b;
			if (outputdpType.indexOf("outputDP") >= 0){
				// here convert scalar(boolean) back to dropbox...
				ConversionFunction.scalarToFile(Integer.toString(c), Utility.pathToConfigFile.substring(0, Utility.pathToConfigFile.lastIndexOf("/")+1) + resultFileName + ".txt");
				String dropboxPath = "/output/" + resultFileName + ".txt";
				System.out.println(dropboxPath);
				String uploadFilePath = Utility.pathToConfigFile.substring(0, Utility.pathToConfigFile.lastIndexOf("/")+1) + resultFileName + ".txt";
				JavaDropbox.uploadToDropbox(dropboxPath, uploadFilePath);
				// getDropbox information such as key and secret...
				String portID = "";
				if (firstdpType.equalsIgnoreCase("Dropbox"))
					portID = portID + "i1"; 
				else
					portID = portID + "i2";
				
				String key = ((DropboxDP) inputPortID_to_DP.get(portID)).dropboxAppKey;
				String secret = ((DropboxDP) inputPortID_to_DP.get(portID)).dropboxAppSecret;
				String token = ((DropboxDP) inputPortID_to_DP.get(portID)).dropboxAppToken;
				String value = key + "," + secret + "," + token + "," + resultFileName + ".txt";
				DataProduct resultDP = new DropboxDP(resultFileName + ".txt", value);
				outputPortID_to_DP.put("o1", resultDP);
				if (registerIntermediateDPs)
					Utility.registerDataProduct(resultDP);
				
			}
			else{
				DataProduct resultDP = new IntegerDP(c, instanceId + ".o1." + runID);
				outputPortID_to_DP.put("o1", resultDP);
				if(registerIntermediateDPs)
					Utility.registerDataProduct(resultDP);
			
			}
			
			System.out.println(instanceId + " producing result: " + c);
		} catch (Exception e) {
		}

	}

	public boolean readyToExecute() {
		if (inputPortID_to_DP == null || inputPortID_to_DP.keySet().size() != 2) {
			//Utility.appendToLog("cannot run Add workflow (" + instanceId + "), number of inputs != 2");
			return false;
		}
		return true;
	}

	@Override
	public void setFinishedStatus() {
		if (!outputPortID_to_DP.isEmpty() && (outputPortID_to_DP.get("o1") instanceof IntegerDP))
			executionStatus = ExecutionStatus.finishedOK;
		else
			executionStatus = ExecutionStatus.finishedError;
	}

}