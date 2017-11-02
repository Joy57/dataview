package builtin;

import implicitshims.ConversionFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import com.dropbox.core.DbxException;

import toyDPM.DataProductManager;
import toyDPM.JavaDropbox;
import translator.ExecutionStatus;
import translator.Experiment;
import translator.Pair;
import translator.SWLAnalyzer;
import translator.Workflow;
import utility.Utility;
import dataProduct.DataProduct;
import dataProduct.DropboxDP;
import dataProduct.RelationalDP;
import dataProduct.StringDP;

/**
 * Builtin workflow
 * 
 * @author Andrey Kashlev
 * 
 */
public class NaturalJoin extends Workflow {
	public NaturalJoin(String instanceid) {
		this.instanceId = instanceid;
		executionStatus = ExecutionStatus.initialized;
	}

	@Override
	public void execute(String runID, boolean registerIntermediateDPs) throws Exception {
		System.out.println("running workflow " + instanceId);
		if (!readyToExecute())
			return;

		Iterator it = inputPortID_to_DP.values().iterator();
		// database name for input relation
		String dbName = null;

		// data product id, or data product name, which is the same as physical table name for the input relation:
		String tableName = null;
		String tableName2 = null;

		// if it is dropbox data product, download file
				// convert file to relation. 
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
			
			String implicitTableName = instanceId + ".input." + ".o1." + runID;
			implicitTableName = implicitTableName.replaceAll("\\.", "ddott");
			ConversionFunction.excelToRelation(implicitTableName, downloadedFilePath);
			System.out.println("#converted excel file into relational table...");
			dbName = Utility.dbNameForRelationalDPs;
			tableName = implicitTableName;
		}
		else{
			if (inputPortID_to_DP.get("i1") instanceof RelationalDP) {
				dbName = ((RelationalDP) inputPortID_to_DP.get("i1")).dbName;
				tableName = ((RelationalDP) inputPortID_to_DP.get("i1")).tableName;
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
			
			String implicitTableName = instanceId + ".input." + ".o2." + runID;
			implicitTableName = implicitTableName.replaceAll("\\.", "ddott");
			ConversionFunction.excelToRelation(implicitTableName, downloadedFilePath);
			System.out.println("#converted excel file into relational table...");
			//dbName2 = Utility.dbNameForRelationalDPs;
			tableName2 = implicitTableName;
			//listOfColumns = ConversionFunction.fileToScalar(downloadedFilePath, "string");
			//System.out.println("#converted file into scalar string...");
		}
		else{
			if (inputPortID_to_DP.get("i2") instanceof RelationalDP) {
				//dbName2 = ((RelationalDP) inputPortID_to_DP.get("i2")).dbName;
				tableName2 = ((RelationalDP) inputPortID_to_DP.get("i2")).tableName;
			}
		}
		// add quotes, e.g. change Hobby=sampts to Hobby='stamps':

		executionStatus = ExecutionStatus.inProcessOfExecution;
		RelationalDP result = null;

		try {
			String resultTableName = instanceId + ".o1." + runID;
			resultTableName = resultTableName.replaceAll("\\.", "ddott");
			Utility.executeNaturalJoin(dbName, tableName, tableName2, resultTableName);
			if (Utility.getAllTableNamesFromDB(dbName).contains(resultTableName)) {
				System.out.println("before outputSch calculation");
				if (outputdpType.indexOf("outputDP") >= 0){
					// here convert relation back to dropbox...
					
					ConversionFunction.relationToExcel(resultTableName, Utility.pathToConfigFile.substring(0, Utility.pathToConfigFile.lastIndexOf("/")+1) + resultTableName + ".xls");
					String dropboxPath = "/output/" + resultTableName + ".xls";
					String uploadFilePath = Utility.pathToConfigFile.substring(0, Utility.pathToConfigFile.lastIndexOf("/")+1) + resultTableName + ".xls";
					JavaDropbox.uploadToDropbox(dropboxPath, uploadFilePath);
					String portID = "";
					if (firstdpType.equalsIgnoreCase("Dropbox"))
						portID = portID + "i1"; 
					else
						portID = portID + "i2";
					
					String key = ((DropboxDP) inputPortID_to_DP.get(portID)).dropboxAppKey;
					String secret = ((DropboxDP) inputPortID_to_DP.get(portID)).dropboxAppSecret;
					String token = ((DropboxDP) inputPortID_to_DP.get(portID)).dropboxAppToken;
					
					String value = key + "," + secret + "," + token + "," + resultTableName + ".xls";
					DataProduct resultDP = new DropboxDP(resultTableName + ".xls", value);
					outputPortID_to_DP.put("o1", resultDP);
					if (registerIntermediateDPs)
						Utility.registerDataProduct(resultDP);					
				}
				else{
					ArrayList<Pair> outputSch = DataProductManager.convertSchemaFromMySQLTypesToViewTypes(Utility
							.getTableSchema(dbName, resultTableName));
					RelationalDP resultDP = new RelationalDP(dbName, resultTableName, DataProductManager.convertSchemaFromMySQLTypesToViewTypes(Utility
							.getTableSchema(dbName, resultTableName)));
					outputPortID_to_DP.put("o1", resultDP);
					if (registerIntermediateDPs)
						Utility.registerDataProduct(resultDP);
				}				
			}
		} catch (Exception e) {
		}

	}

	public boolean readyToExecute() {
		if (inputPortID_to_DP == null || inputPortID_to_DP.keySet().size() != 2) {
			// Utility.appendToLog("cannot run Add workflow (" + instanceId + "), number of inputs != 2");
			return false;
		}
		return true;
	}

	@Override
	public void setFinishedStatus() {
		if (!outputPortID_to_DP.isEmpty() && (outputPortID_to_DP.get("o1") instanceof RelationalDP))
			executionStatus = ExecutionStatus.finishedOK;
		else
			executionStatus = ExecutionStatus.finishedError;
	}

}