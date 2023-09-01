import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Set;
import java.util.Map;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Collection;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class Server implements ProjectLib.CommitServing, ProjectLib.MessageHandling {
	public static final Object lock = new Object();
	public static final long timeout = 6000;
	public static final String logFilePath = "log.txt";
	public static ProjectLib PL;
	//the counter that identifies how many potantial collages have been proposed
	public static int cnt = 0;
	//a flag that indicates the Server is currently in recovery. If set, will block all messages 
	public static boolean inRecovery = true;
	//maps the ith proposal collage request to its correponding UserNode vote map
	public static ConcurrentHashMap<Integer, ConcurrentHashMap<String, String>> voteMap;
	//maps the reference number(a potential collage) to its commit/abort decision
	public static ConcurrentHashMap<Integer, String> decisionMap;
	
	/*
	 * Construct a message to send to the UserNodes.
	 * 
	 * 	dest: a UserNode this message is directed to
	 * 	type: "PREPARE", "COMMIT", "ABORT", "INPROGRESS"
	 * 	refNum: the reference number associated with a certain proposal collage
	 * 	filenames: the name of the file if the collage was to be commited
	 * 	img: the iamge bytes
	 * 
	 * 	returns: the constructed message has the format
	 * 		
	 * 		totalContentLen(4bytes)PREPARE,14,file:Names:carnival:,imageBytes92345930530..
	 * 		totalContentLen(4bytes)COMMIT,14, , 
	 * 		totalContentLen(4bytes)ABORT,14, ,
	 * 		totalContentLen(4bytes)INPROGRESS,14, ,
	 * 		
	 * 	where totalContentLen(4bytes) includes itself and the last ,
	*/
	public ProjectLib.Message getMsg(String dest, String type, int refNum, String filenames, byte[] img) {
		//construct necessary component of the message
		byte[] header = (type+","+refNum+",").getBytes();
		int headerLen = header.length;
		byte[] filenameByte = (filenames+",").getBytes();
		int filenameByteLen = filenameByte.length;
		int totalContentLen = headerLen + filenameByteLen + 4;
		
		//allocate the buffer
		int totalLen = totalContentLen + img.length;
		byte[] bstream = new byte[totalLen];

		//copy contents into the buffer
		byte[] totalContentLenBytes = ByteBuffer.allocate(4).putInt(totalContentLen).array();
		System.arraycopy(totalContentLenBytes, 0, bstream, 0, 4);
		System.arraycopy(header, 0, bstream, 4, headerLen);
		System.arraycopy(filenameByte, 0, bstream, headerLen+4, filenameByteLen);
		System.arraycopy(img, 0, bstream, totalContentLen, img.length);
		
		//construct the message object
		ProjectLib.Message msg = new ProjectLib.Message( dest, bstream );

		return msg;
	}

	/*
	 * Construct a byte[] to be written to the log file
	 * 
	 * 	refnum: the reference number associated with a certain proposal collage
	 * 	decision: "COMMIT", "ABORT"
	 * 	collageName: the name of the file if the collage was to be commited
	 * 	img: the iamge bytes
	 * 
	 * 	returns: the constructed byte stream has the format
	 * 
	 * 		totalLen(4bytes)totalContentLen(4bytes)14:COMMIT:collageName1:imageBytes92345930530......totalLen(4bytes)
	 * 
	 * 	where totalLen includes itself to the last byte before next totalLen
	 * 		  totalContentLen includes itself to the : after collageName1
	 */

	public byte[] getLogBytes(int refnum, String decision, String collageName, byte[] img) {
		//construct necessary components of the log
		byte[] content = (refnum + ":" + decision + ":" + collageName + ":").getBytes();
		int totalContentLen = content.length + 4;
		int totalLen = totalContentLen + img.length + 4;

		//allocate the buffer
		byte[] bstream = new byte[totalLen];

		//construct necessary components of the log
		byte[] totalLenBytes = ByteBuffer.allocate(4).putInt(totalLen).array();
		byte[] totalContentLenBytes = ByteBuffer.allocate(4).putInt(totalContentLen).array();
		
		//write the contents into the buffer
		System.arraycopy(totalLenBytes, 0, bstream, 0, 4);
		System.arraycopy(totalContentLenBytes, 0, bstream, 4, 4);
		System.arraycopy(content, 0, bstream, 8, totalContentLen-4);
		System.arraycopy(img, 0, bstream, totalContentLen+4, img.length);

		return bstream;
	}

	public void startCommit( String filename, byte[] img, String[] sources ) {
		int sourcesLen = sources.length;
		int refNum;
		
		//need the lock to ensure each proposal collage gets a unique reference number
		synchronized (lock) {
			refNum = cnt;
			cnt += 1;
		}

		//maps the UserNode to its corresponding files involved in the current proposal collage
		ConcurrentHashMap<String, String> userFilesMap = new ConcurrentHashMap<>();

		//construct the map
		for (int i = 0; i<sourcesLen; i++) {
			String pair = sources[i];
			String[] sfPair = pair.split(":");
			if (userFilesMap.containsKey(sfPair[0])) {
				String temp = userFilesMap.get(sfPair[0]);
				userFilesMap.put(sfPair[0], temp + sfPair[1] + ":");
			} else {
				userFilesMap.put(sfPair[0], sfPair[1] + ":");
			}
		}

		//maps the UserNode to its opinion on the collage involved.
		ConcurrentHashMap<String, String> responseMap = new ConcurrentHashMap<>();
		voteMap.put(refNum, responseMap);

		//send messages to UserNode ask them to prepare
		for (String key : userFilesMap.keySet()) {
			String filenames = userFilesMap.get(key);
			ProjectLib.Message msg = getMsg(key, "PREPARE", refNum, filenames, img);
			PL.sendMessage( msg );
		}

		//record the current time to later timeout
		long startTime = System.currentTimeMillis();

		int userNum = userFilesMap.size();
		//wait until either timeout of get all the responses from UserNodes involved
		while (responseMap.size() < userNum && System.currentTimeMillis() - startTime < timeout) {
			//do nothing, wait unitl either get all the responses or timeout;
		}

		//figure out if can commit
		boolean result = true;
		
		//didn't get all the responses, timeout
		if (responseMap.size() < userNum) {
			result = false;
		}
		
		//get all the responses, but some said NO
		for (String key : responseMap.keySet()) {
			if (!responseMap.get(key).equals("YES")) {
				result = false;
				break;
			}
		}

		//get all the responses and they all said YES, ready to commit
		if (result) {
			try {
				//before taking any action, first write the log
				FileOutputStream fos = new FileOutputStream(logFilePath, true);
				byte[] bstream = getLogBytes(refNum, "COMMIT", filename, img);
				fos.write(bstream);
				fos.close();
				PL.fsync();
				
				//write the decision to decisionMap
				decisionMap.put(refNum, "COMMIT");
				//commit and broadcast results
				commiT(filename, img, userFilesMap, refNum);
			} catch ( IOException e ) {
				System.err.println("IOException");
			}
		} else {
			//should abort
			try {
				//before taking any action, first write the log
				FileOutputStream fos = new FileOutputStream(logFilePath, true);
				byte[] placeHolder = {0x00};
				byte[] bstream = getLogBytes(refNum, "ABORT", " ", placeHolder);
				fos.write(bstream);
				fos.close();
				PL.fsync();

				//write the decision to decisionMap
				decisionMap.put(refNum, "ABORT");
				//abort and broadcast results
				aborT(userFilesMap, refNum);		
			} catch ( IOException e) {
				System.err.println("IOException");
			}
		}

		//after broadcasting results, the responseMap is no longer needed, so delete it
		voteMap.remove(refNum);
	}

	//commit a collage, and broadcast the results
	public void commiT(String filename, byte[] img, ConcurrentHashMap<String, String> userFilesMap, int refNum) {
		//write the file into local directory
		try {
			FileOutputStream outputStream = new FileOutputStream(filename);
			outputStream.write(img);
      		outputStream.close();
		} catch (IOException e) {
			System.err.println("IOException");
		}

		//broadcast the results of the proposal collage to the UserNodes
		for (String user : userFilesMap.keySet()) {
			byte[] placeHolder = {0x00};
			ProjectLib.Message msg = getMsg(user, "COMMIT", refNum, " ", placeHolder);
			PL.sendMessage(msg);
		}
	}

	//abort a collage, and broadcast the results
	public void aborT(ConcurrentHashMap<String, String> userFilesMap, int refNum) {
		//broadcast the results of the proposal collage to the UserNodes
		for (String user : userFilesMap.keySet()) {
			byte[] placeHolder = {0x00};
			ProjectLib.Message msg = getMsg(user, "ABORT", refNum, " ", placeHolder);
			PL.sendMessage(msg);
		}
	}

	/*
	 * process messages from the UserNodes
	 * the message is of the form:
	 * 		RESPONSE,14,YES
	 * 		RESPONSE,14,NO
	 * 		ASK,14, 
	 */

	public boolean deliverMessage( ProjectLib.Message msg ) {
		//parse the message
		String source = msg.addr;
		byte[] bytes = msg.body;
		String response = new String(bytes);
		String[] info = response.split(",");

		//process the message
		if (info[0].equals("RESPONSE")) {
			String refNum = info[1];
			String answer = info[2];
			//get the corresponding responseMap
			ConcurrentHashMap<String, String> responseMap = voteMap.get(Integer.parseInt(refNum));
			String key = source;
			//the responseMap may have already been deleted due to timeout
			if (responseMap != null) {
				//put response in if it's not timeout and the map is still there
				responseMap.put(key,answer);
			}
		} else if (info[0].equals("ASK")) {
			byte[] placeHolder = {0x00};
			String refNum = info[1];
			String decision;

			while (inRecovery) {
				//wait until Server recovery finishes;
			}

			//it's possible that the reference number is not presented in decisionMap due to faliure, so use a default value
			String temp = decisionMap.getOrDefault(Integer.parseInt(refNum), "NF");
			if (temp.equals("NF")) {
				//if it's not presented in the decisionMap, then either the Server failed before commiting it, or the server is currently gathering responses
				if (voteMap.containsKey(Integer.parseInt(refNum))) {
					//when the server is currently gathering responses, tell the UserNode to wait
					decision = "INPROGRESS";
				} else {
					//when the Server failed before commiting it, tell the UserNode to abort
					decision = "ABORT";
				}
			} else {
				//it's presented in the decision map, return whatever it is
				decision = temp;
			}

			//send messages to the UserNode that asked for it
			ProjectLib.Message reply = getMsg(source, decision,Integer.parseInt(refNum), " ", placeHolder);
			PL.sendMessage(reply);
		} else {
			//unrecognizable messages
			System.err.println("deliverMessage: not RESPONSE, forward to PL.getMessage()");
			return false;
		}
		
		return true;
	}	

	//the recovery code
	public static void doRecovery() {
		File file = new File(logFilePath);
		if (!file.exists()) {
			//no need to recover, just create a log.txt file
			try {
				String initialLog = "";
				file.createNewFile();
				FileOutputStream fos = new FileOutputStream(file);
				fos.write(initialLog.getBytes());
				fos.close();
				PL.fsync();
				return;
			} catch (IOException e) {
				System.err.println("IOException");
			}
		}
		if (file.length() == 0) {
			//nothing in the log, no need to recover.
			return;
		}

		//read the log file into fileBytes
		byte[] fileBytes = {0x00};
		try {
			FileInputStream fis = new FileInputStream(logFilePath);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int bytesRead;
			while ((bytesRead = fis.read(buffer)) != -1) {
				bos.write(buffer, 0, bytesRead);
			}
			fileBytes = bos.toByteArray();
			fis.close();
			bos.close();
		} catch (IOException e) {
			System.err.println("IOException");
		}

		//recover the decision map, and commit (if not already commited) all the collages.
		
		//records how many bytes have been read. read until the end of the file
		int counter = 0;
		
		while (counter < fileBytes.length) {
			//get a piece of log
			byte[] firstFour = Arrays.copyOfRange(fileBytes, counter, counter+4);
			int totalLen = ByteBuffer.wrap(firstFour).getInt();
			byte[] four2eight = Arrays.copyOfRange(fileBytes, counter+4, counter+8);
			int totalContentLen = ByteBuffer.wrap(four2eight).getInt();
			byte[] contentBytes = Arrays.copyOfRange(fileBytes, counter+8, counter+4+totalContentLen-1);

			String contentString = new String(contentBytes);
			String[] contents = contentString.split(":");
			
			//parse the log information
			int refNum = Integer.parseInt(contents[0]);
			String decision = contents[1];
			String collageName = contents[2];
			byte[] img = {0x00};
			//need img only if it's a COMMIT message
			if (decision.equals("COMMIT")) {
				img = Arrays.copyOfRange(fileBytes, counter+4+totalContentLen, counter+totalLen);
			}
			
			//if it's a COMMIT message, try re-save the file if it's not already saved
			if (decision.equals("COMMIT")) {
				try {
					File f = new File(collageName);
					if (!f.exists()) {
						FileOutputStream outputStream = new FileOutputStream(collageName);
						outputStream.write(img);
						outputStream.close();
					}
				} catch (IOException e) {
					System.err.println("IOException");
				}
			}

			//recover the decisionMap
			decisionMap.put(refNum, decision);
			counter += totalLen;
		}
		
		//to recover the cnt
		int m = -1;
		//cnt needs to be largest than any commited/aborted reference number to avoid collision 
		for (int key : decisionMap.keySet()) {
			if (key > m) {
				m = key;
			}
		}
		cnt = m+1;
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv, srv );
		voteMap = new ConcurrentHashMap<>();
		decisionMap = new ConcurrentHashMap<>();

		//do recovery, while blocking all the messages from UserNodes
		doRecovery();
		//release the flag
		inRecovery = false;

		//begin listening to the UserNodes
		while (true) {
			//wait for collage proposals
		}
	}
}