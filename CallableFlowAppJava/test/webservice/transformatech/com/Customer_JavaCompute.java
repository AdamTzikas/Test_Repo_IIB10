package test.webservice.transformatech.com;

import java.sql.*;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;
import com.ibm.broker.plugin.MbXMLNSC;

public class Customer_JavaCompute extends MbJavaComputeNode {

	@SuppressWarnings("null")
	public void evaluate(MbMessageAssembly inAssembly) throws MbException {
		MbOutputTerminal out = getOutputTerminal("out");
		MbOutputTerminal alt = getOutputTerminal("alternate");

		MbMessage inMessage = inAssembly.getMessage();
		MbMessageAssembly outAssembly = null;
		try {
			// create new message as a copy of the input
			MbMessage outMessage = new MbMessage(inMessage);
			
			// ----------------------------------------------------------
			// Add user code below
			
			
			MbElement mbID = inMessage.getRootElement().getFirstChild().getNextSibling().getNextSibling().getFirstChild().getFirstChild().getNextSibling();
			String strID = mbID.getValueAsString();
			int iID = Integer.parseInt(strID);
			String strFirstName = new String();
			String strLastName = new String();
			String strAddress = new String();
			String strZip = new String();
			String strCity = new String();
			String strState = new String();
			String strSuccess = new String();
			
			Statement SQL;
			ResultSet results;
			
			System.out.println("Starting Service");
			
			//Build SQL Statement
			String strSQL = "SELECT * FROM SAMPLE.ADAM.CUSTOMERS WHERE (\"CustomerID\" = '"+ iID + "')";
			//Build connection to DB2
//			String url = "jdbc:db2://localhost:50000/SAMPLE";
//			String user = "db2admin";
//			String password = "db2admin";
//			String jdbcClassName="com.ibm.db2.jcc.DB2Driver";
			Connection connection = null;
			 
			//call the JDBC class and attempt a connection
			//Class.forName(jdbcClassName);
			connection =  getJDBCType4Connection("SAMPLE",
			         JDBC_TransactionType.MB_TRANSACTION_AUTO);;
		     
			
			if (connection != null){
				System.out.println("Connected to DB2");
			}
			else{
				System.out.println("Not connected to DB2");
			}
			
			try {
				connection.setAutoCommit(false);
				System.out.println("Creating SQL");
				SQL = connection.createStatement();
				System.out.println("Executing SQL");	
				results = SQL.executeQuery(strSQL);
				System.out.println("Returning results SQL");
				//iterate over the results set and set results to strings
				while (results.next()){
					strFirstName = results.getString(2);
					strLastName = results.getString(3);
					strAddress = results.getString(4);
					strCity = results.getString(5);
					strState = results.getString(6);
					strZip = results.getString(7);
	    			}
				if(strFirstName.equals("") || strFirstName == null)
				{
					strSuccess = "Failed to find customer";
				}
				else{
				strSuccess = "Success";
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} 
			//Build output XML
			outMessage.getRootElement().getLastChild().delete();
			MbElement outRoot = outMessage.getRootElement();
			MbElement outBody = outRoot.createElementAsLastChild(MbXMLNSC.PARSER_NAME);
			//create the root tag
			MbElement customerOut = outBody.createElementAsFirstChild(MbElement.TYPE_NAME);
			customerOut.setName("Customer");
			//create elements
			customerOut.createElementAsFirstChild(MbElement.TYPE_NAME_VALUE, "CustomerID", strID);
			customerOut.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "FirstName", strFirstName);
			customerOut.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "LastName", strLastName);
			//Add address element
			MbElement addressOut = customerOut.createElementAsLastChild(MbElement.TYPE_NAME);
			addressOut.setName("Address");
			addressOut.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Street", strAddress);
			addressOut.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "City", strCity);
			addressOut.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "State", strState);
			addressOut.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Zip", strZip);
			
			//add success/fail tag
			customerOut.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Success", strSuccess);
			
			outAssembly = new MbMessageAssembly(inAssembly, outMessage);
			// End of user code
			// ----------------------------------------------------------
		} catch (MbException e) {
			// Re-throw to allow Broker handling of MbException
			throw e;
		} catch (RuntimeException e) {
			// Re-throw to allow Broker handling of RuntimeException
			throw e;
		} catch (Exception e) {
			// Consider replacing Exception with type(s) thrown by user code
			// Example handling ensures all exceptions are re-thrown to be handled in the flow
			throw new MbUserException(this, "evaluate()", "", "", e.toString(),
					null);
		}
		// The following should only be changed
		// if not propagating message to the 'out' terminal
		out.propagate(outAssembly);

	}

}
