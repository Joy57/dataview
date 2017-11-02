/**
 * This is the KNN algorithm.
 * @author  Aravind Mohan, Ishtiaq Ahmed. 
 */
package builtin;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Iterator;

import translator.ExecutionStatus;
import translator.Workflow;
import utility.Utility;
import webbench.WebbenchUtility;
import dataProduct.DataProduct;
import dataProduct.IntegerDP;
import dataProduct.RelationalDP;
import dataProduct.StringDP;
import dataProduct.FileDP;
import datamining.KNN;
import implicitshims.ConversionFunction;

import java.util.HashSet;

import com.dropbox.core.DbxException;

import toyDPM.JavaDropbox;
import translator.Experiment;
import translator.Pair;
import translator.SWLAnalyzer;
import dataProduct.DropboxDP;

public class knn extends Workflow {
	public knn(String instanceid) {
		this.instanceId = instanceid;
		executionStatus = ExecutionStatus.initialized;
	}

	@Override
	public void execute(String runID, boolean registerIntermediateDPs) throws Exception {
		System.out.println("running workflow " + instanceId);
		if (!readyToExecute())
			return;

		Iterator it = inputPortID_to_DP.values().iterator();

		String strFileName1 = null;
		String strFilePath1 = null;

		String strFileName2 = null;
		String strFilePath2 = null;

		int valueOfK = 0;

		// if it is dropbox data product, download file
		// convert file to relation.
		String filename1 = null;
		String filename2 = null;
		String filename3 = null;
		String firstdpType = null;
		String seconddpType = null;
		String thirddpType = null;
		String outputdpType = null;
		// get input data mapped to input port?
		HashSet<String> inputMapping1 = new HashSet<String>();
		inputMapping1 = SWLAnalyzer.getInputMappingFromAttribute(
				Experiment.workflowSpec, this.instanceId + ".i1");

		if (inputMapping1 == null || inputMapping1.size() == 0) {
			inputMapping1 = SWLAnalyzer.getDataChannelsFromAttribute(
					Experiment.workflowSpec, this.instanceId, "i1");
		}

		HashSet<String> inputMapping2 = new HashSet<String>();
		inputMapping2 = SWLAnalyzer.getInputMappingFromAttribute(
				Experiment.workflowSpec, this.instanceId + ".i2");

		if (inputMapping2 == null || inputMapping2.size() == 0) {
			inputMapping2 = SWLAnalyzer.getDataChannelsFromAttribute(
					Experiment.workflowSpec, this.instanceId, "i2");
		}
		
		HashSet<String> inputMapping3 = new HashSet<String>();
		inputMapping3 = SWLAnalyzer.getInputMappingFromAttribute(
				Experiment.workflowSpec, this.instanceId + ".i3");

		if (inputMapping3 == null || inputMapping3.size() == 0) {
			inputMapping3 = SWLAnalyzer.getDataChannelsFromAttribute(
					Experiment.workflowSpec, this.instanceId, "i3");
		}

		for (String mapping : inputMapping1) {
			if (mapping.indexOf("_") >= 0)
				mapping = mapping.substring(5, mapping.indexOf("_"));
			firstdpType = Utility.getDPLTypeFromDB(mapping);
		}
		for (String mapping : inputMapping2) {
			if (mapping.indexOf("_") >= 0)
				mapping = mapping.substring(5, mapping.indexOf("_"));
			seconddpType = Utility.getDPLTypeFromDB(mapping);
		}
		for (String mapping : inputMapping3) {
			System.out.println("input mapping 3");
			System.out.println(mapping);
			if (mapping.indexOf("_") >= 0)
				mapping = mapping.substring(5, mapping.indexOf("_"));
			thirddpType = Utility.getDPLTypeFromDB(mapping);
			System.out.println(thirddpType);
		}

		System.out.println(Utility.nodeToString(Experiment.workflowSpec));

		HashSet<String> outputMapping = new HashSet<String>();
		outputMapping = SWLAnalyzer.getOutputMappingToAttribute(
				Experiment.workflowSpec, this.instanceId + ".o1");

		if (outputMapping == null || outputMapping.size() == 0) {
			System.out.println("getting data channel information");
			System.out.println(this.instanceId);
			outputMapping = SWLAnalyzer.getDataChannelsToAttribute(
					Experiment.workflowSpec, this.instanceId, "o1");
		}

		for (String mapping : outputMapping) {
			System.out.println(mapping);
			outputdpType = mapping.substring(mapping.indexOf("TO_") + 3,
					mapping.length());

		}
		System.out.println(firstdpType);
		System.out.println(seconddpType);
		// get dpl type of the input data?
		if (firstdpType.equalsIgnoreCase("Dropbox")) {
			filename1 = ((DropboxDP) inputPortID_to_DP.get("i1")).dropboxPath;
			String key = ((DropboxDP) inputPortID_to_DP.get("i1")).dropboxAppKey;
			String secret = ((DropboxDP) inputPortID_to_DP.get("i1")).dropboxAppSecret;
			String token = ((DropboxDP) inputPortID_to_DP.get("i1")).dropboxAppToken;
			JavaDropbox.initializeDropbox(key, secret, token);
			String dropboxPath = filename1;
			System.out.println(dropboxPath);
			filename1 = filename1.substring(filename1.lastIndexOf("/") + 1,
					filename1.length());
			System.out.println(filename1);
			String downloadedFilePath = Utility.pathToConfigFile.substring(0,
					Utility.pathToConfigFile.lastIndexOf("/") + 1) + filename1;
			JavaDropbox.downloadFromDropbox(dropboxPath, downloadedFilePath);
			System.out.println("#downloaded file from dropbox..");
			strFileName1 = filename1;
			strFilePath1 = Utility.pathToConfigFile.substring(0,
					Utility.pathToConfigFile.lastIndexOf("/") + 1);
		} else {
			if (inputPortID_to_DP.get("i1") instanceof FileDP) {
				strFileName1 = ((FileDP) inputPortID_to_DP.get("i1")).fileName;
				strFilePath1 = ((FileDP) inputPortID_to_DP.get("i1")).filePath;

			}
		}

		if (seconddpType.equalsIgnoreCase("Dropbox")) {
			filename2 = ((DropboxDP) inputPortID_to_DP.get("i2")).dropboxPath;
			String key = ((DropboxDP) inputPortID_to_DP.get("i2")).dropboxAppKey;
			String secret = ((DropboxDP) inputPortID_to_DP.get("i2")).dropboxAppSecret;
			String token = ((DropboxDP) inputPortID_to_DP.get("i2")).dropboxAppToken;
			JavaDropbox.initializeDropbox(key, secret, token);
			String dropboxPath = filename2;
			System.out.println(dropboxPath);
			filename2 = filename2.substring(filename2.lastIndexOf("/") + 1,
					filename2.length());
			System.out.println(filename2);

			String downloadedFilePath = Utility.pathToConfigFile.substring(0,
					Utility.pathToConfigFile.lastIndexOf("/") + 1) + filename2;
			JavaDropbox.downloadFromDropbox(dropboxPath, downloadedFilePath);

			strFileName2 = filename2;
			strFilePath2 = Utility.pathToConfigFile.substring(0,
					Utility.pathToConfigFile.lastIndexOf("/") + 1);
		} else {
			if (inputPortID_to_DP.get("i2") instanceof FileDP) {
				strFileName2 = ((FileDP) inputPortID_to_DP.get("i2")).fileName;
				strFilePath2 = ((FileDP) inputPortID_to_DP.get("i2")).filePath;
			}
		}
		
		if (thirddpType.equalsIgnoreCase("Dropbox")){
			System.out.println("inside third data product...");
			filename3 = ((DropboxDP) inputPortID_to_DP.get("i3")).dropboxPath;
			System.out.println(filename3);
			String key = ((DropboxDP) inputPortID_to_DP.get("i3")).dropboxAppKey;
			String secret = ((DropboxDP) inputPortID_to_DP.get("i3")).dropboxAppSecret;
			String token = ((DropboxDP) inputPortID_to_DP.get("i3")).dropboxAppToken;
			JavaDropbox.initializeDropbox(key, secret, token);
			String dropboxPath = filename3;
			System.out.println(dropboxPath);
			filename3 = filename3.substring(filename3.lastIndexOf("/")+1, filename3.length());
			System.out.println(filename3);
			
			String downloadedFilePath = Utility.pathToConfigFile.substring(0, Utility.pathToConfigFile.lastIndexOf("/")+1) + filename3;
			JavaDropbox.downloadFromDropbox(dropboxPath, downloadedFilePath);
			System.out.println("#downloaded file from dropbox..");
			
			valueOfK = Integer.parseInt(ConversionFunction.fileToScalar(downloadedFilePath, "integer").trim());
			System.out.println("#converted file into scalar integer...");
		}
		else{
			if (inputPortID_to_DP.get("i3") instanceof IntegerDP) {
				valueOfK = ((IntegerDP) inputPortID_to_DP.get("i3")).data;
			}
		}
		
		executionStatus = ExecutionStatus.inProcessOfExecution;
		try {
			
			String outputFilePath = Utility.pathToConfigFile.substring(0, Utility.pathToConfigFile.lastIndexOf("/")+1);
			String outputFileName = instanceId + runID + "_outputfile.txt";
			String outputFile = outputFilePath + outputFileName;
			strFilePath1 = strFilePath1 + strFileName1;
			strFilePath2 = strFilePath2 + strFileName2;
			System.out.println(strFilePath1);
			System.out.println(strFilePath2);
			System.out.println(outputFilePath);
			System.out.println(outputFile);

			KNN.knnImplementor(valueOfK, strFilePath1, strFilePath2, outputFile);
			
			if (outputdpType.indexOf("outputDP") >= 0){
				// here convert relation back to dropbox...
				String dropboxPath = "/output/" + outputFileName;
				System.out.println(dropboxPath);
				JavaDropbox.uploadToDropbox(dropboxPath, outputFile);
				// getDropbox information such as key and secret...
				String portID = "";
				if (firstdpType.equalsIgnoreCase("Dropbox"))
					portID = portID + "i1"; 
				else
					portID = portID + "i2";
				
				String key = ((DropboxDP) inputPortID_to_DP.get(portID)).dropboxAppKey;
				String secret = ((DropboxDP) inputPortID_to_DP.get(portID)).dropboxAppSecret;
				String token = ((DropboxDP) inputPortID_to_DP.get(portID)).dropboxAppToken;
				String value = key + "," + secret + "," + token + "," + outputFileName;
				DataProduct resultDP = new DropboxDP(outputFileName, value);
				outputPortID_to_DP.put("o1", resultDP);
				if (registerIntermediateDPs)
					Utility.registerDataProduct(resultDP);
				
			}
			else{
				FileDP resultDP = new FileDP(outputFileName, instanceId + ".o1."
						+ runID, "txt", outputFilePath, "Server");
				outputPortID_to_DP.put("o1", resultDP);
				if (registerIntermediateDPs)
					Utility.registerDataProduct(resultDP);
							
			}

		} catch (Exception e) {
		}

	}

	public boolean readyToExecute() {
		if (inputPortID_to_DP == null || inputPortID_to_DP.keySet().size() != 3) {
			// Utility.appendToLog("cannot run Add workflow (" + instanceId +
			// "), number of inputs != 2");
			return false;
		}
		return true;
	}

	@Override
	public void setFinishedStatus() {
		if (!outputPortID_to_DP.isEmpty()
				&& (outputPortID_to_DP.get("o1") instanceof FileDP))
			executionStatus = ExecutionStatus.finishedOK;
		else
			executionStatus = ExecutionStatus.finishedError;
	}

}
