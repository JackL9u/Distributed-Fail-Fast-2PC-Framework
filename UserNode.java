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

public class UserNode implements ProjectLib.MessageHandling {
	public final String myId;
	//maps the locked resource to the refNum that locks it
	public ConcurrentHashMap<String, Integer> resourceMap;
	public ProjectLib PL;
	public Object lock;
	public String logFilePath;
	//a flag that indicates the UserNode is currently in recovery. If set, will block all messages 
	public boolean inRecovery;

	//constructor
	public UserNode( String id ) {
		myId = id;
		resourceMap = new ConcurrentHashMap<>();
		lock = new Object();
		logFilePath = "log.txt";
		inRecovery = true;
	}

	/*
	 * message is of the form:
	 * 		RESPONSE,14,YES
	 * 		RESPONSE,14,NO
	 * 		ASK,14, 
	 */
	public ProjectLib.Message getMsg(String type, int refNum, String answer) {
		byte[] bstream = (type+","+refNum+","+answer).getBytes();
		ProjectLib.Message msg = new ProjectLib.Message( "Server", bstream );
		return msg;
	}

	//accept a certain collage, and send the responses to the Server
	public void accepT(int refNum) {
		ProjectLib.Message msg = getMsg("RESPONSE", refNum, "YES");
		PL.sendMessage( msg );
	}

	//ask the server the result of a certain reference number
	public void asK(int refNum) {
		ProjectLib.Message msg = getMsg("ASK", refNum, " ");
		PL.sendMessage( msg );
	}

	//deny a certain collage, and send the responses to the Server
	public void denY(int refNum) {
		//and release all the resources occupied by the proposal
		for (String key : resourceMap.keySet()) {
			if (resourceMap.getOrDefault(key,-1) == refNum) {	
				try {
					//before taking any action, write to the log
					String logMessage = key+":"+"UNLOCK"+":"+refNum+",";
					byte[] logBytes = logMessage.getBytes();
					FileOutputStream fos = new FileOutputStream(logFilePath, true);
					fos.write(logBytes);
					fos.close();
					PL.fsync();
				} catch (IOException e) {
					System.err.println("IOException");
				}

				//release the resource
				resourceMap.remove(key);
			}
		}

		//send the response to the server
		ProjectLib.Message msg = getMsg("RESPONSE", refNum, "NO");
		PL.sendMessage( msg );
	}

	/*
	 * recover the resourceMap, pairs are of the form
	 * 		filename1:LOCK:2
	 */
	public void recoverMap(String[] fileRefpairs) {
		//loop through all the pairs
		for (String pair : fileRefpairs) {
			//maps out the content
			String[] p = pair.split(":");
			String filename = p[0];
			String status = p[1];
			int refnum = Integer.parseInt(p[2]);
			
			if (status.equals("LOCK")) {
				//put the resource in lock
				resourceMap.put(filename, refnum);
			} else if (status.equals("UNLOCK")) {
				//unlock the resource
				if (!resourceMap.containsKey(filename)) {
					//this should never happen.
					System.err.println("error happening, resource is already unlocked");
					return;
				}
				//release the recourse
				resourceMap.remove(filename);
			} else if (status.equals("DELETE")) {
				File f = new File(filename);
				//check if the file is already deleted
				if (f.exists()) {
					//if not, delete it now
					f.delete();
				}
			} else {
				System.err.println("recoverMap: unknown log message");
			}
		}
	}

	//the recovery code
	public void userRecover() {
		File file = new File(logFilePath);
		if (!file.exists()) {
			//no need to recover, just create the log.txt file
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
			//nothing there, no need to recover.
			return;
		}
		
		//read the file into fileBytes
		int bytesRead;
		byte[] fileBytes = {0x00};
		try {
			FileInputStream fis = new FileInputStream(logFilePath);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			
			while ((bytesRead = fis.read(buffer)) != -1) {
				bos.write(buffer, 0, bytesRead);
			}
			fileBytes = bos.toByteArray();
			fis.close();
			bos.close();
		} catch (IOException e) {
			System.err.println("IOException");
		}

		//recover the map
		byte[] mapContent = fileBytes;
		String infoString = new String(mapContent);
		infoString = infoString.substring(0, infoString.length() - 1);
		String[] fileRefpairs = infoString.split(",");
		recoverMap(fileRefpairs);

		//ask the decision for every reference number on which there is a resource locked
		Collection<Integer> values = resourceMap.values();
		Set<Integer> keys = new HashSet<>(values);
		for (int refnum : keys) {
			asK(refnum);
		}
		
		return;
	}

	/*
	 * the message is of the form:
	 * 		RESPONSE,14,YES
	 * 		RESPONSE,14,NO
	 * 		ASK,14, 
	 */
	public boolean deliverMessage( ProjectLib.Message msg ) {
		while (inRecovery) {
			//wait until recovery finishes
		}
		String source = msg.addr;
		
		if (!source.equals("Server")) {
			System.err.println("deliverMessage: not from Server, forward to PL.getMessage()");
			return false;
		}

		//parse the message
		byte[] bytes = msg.body;
		byte[] firstFourBytes = Arrays.copyOfRange(bytes, 0, 4);
		int totalContentLen = ByteBuffer.wrap(firstFourBytes).getInt();
		byte[] contentBytes = Arrays.copyOfRange(bytes, 4, totalContentLen);
		
		String content = new String(contentBytes);
		content = content.substring(0, content.length() - 1);
		byte[] img = Arrays.copyOfRange(bytes, totalContentLen, bytes.length); //

		String[] info = content.split(",");
		String type = info[0];
		int refNum = Integer.parseInt(info[1]);
		String filenamess = info[2];
		
		//process the message
		if (type.equals("PREPARE")) {
			//find all the files involved in this collage
			filenamess = filenamess.substring(0, filenamess.length() - 1);
			String[] filenames = filenamess.split(":");
			
			//need to synchronize in case of concurrent requests
			synchronized (lock) {
				for (String filename : filenames) {
					File file = new File(filename);
					//if the UserNode doesn't won the file
					if (!file.exists()) {
						System.err.println("deliverMessage: file doesn't exist");
						denY(refNum);
						return true;
					}
					//if the resource has already by occupied
					if (resourceMap.getOrDefault(filename,-1) >= 0) {
						System.err.println("deliverMessage: resource already occupied");
						denY(refNum);
						return true;
					}

					//otherwise, ask the user if it's ok

					//before taking any action, first write the log
					try {
						String logMessage = filename+":"+"LOCK"+":"+refNum+",";
						byte[] logBytes = logMessage.getBytes();
						FileOutputStream fos = new FileOutputStream(logFilePath, true);
						fos.write(logBytes);
						fos.close();
						PL.fsync();
					} catch (IOException e) {
						System.err.println("IOException");
					}

					//need to lock the resource in case the user accepts the collage
					resourceMap.put(filename, refNum);
				}	
			}
			//ask the user its opinion
			boolean result = PL.askUser(img, filenames);
			//if NO, deny it and tell the Server
			if (!result) {
				denY(refNum);
				return true;
			}
			//otherwise accept if and tell the Server
			accepT(refNum);
			return true;
		} else if (type.equals("COMMIT")) {
			//need to delete the files involved
			for (String key : resourceMap.keySet()) {
				//find all the resources associated with the reference number and delete it
				if (resourceMap.getOrDefault(key,-1) == refNum) {
					//before taking any action, first write the log
					try {
						String logMessage = key+":"+"DELETE"+":"+refNum+",";
						//System.err.println("UserNode "+myId+" deleted "+key+" due to commit "+refNum);
						byte[] logBytes = logMessage.getBytes();
						FileOutputStream fos = new FileOutputStream(logFilePath, true);
						fos.write(logBytes);
						fos.close();
						PL.fsync();
					} catch (IOException e) {
						System.err.println("IOException");
					}

					//delete the file
					File file = new File(key);
					file.delete();
					resourceMap.remove(key);
				}
			}
		} else if (type.equals("ABORT")) {
			//need to release the resources
			for (String key : resourceMap.keySet()) {
				//find all the resources associated with the reference number and release it
				if (resourceMap.getOrDefault(key,-1) == refNum) {
					//before taking any action, first write the log
					try {
						String logMessage = key+":"+"UNLOCK"+":"+refNum+",";
						//System.err.println("UserNode "+myId+" unlocked "+key+" due to abort "+refNum);
						byte[] logBytes = logMessage.getBytes();
						FileOutputStream fos = new FileOutputStream(logFilePath, true);
						fos.write(logBytes);
						fos.close();
						PL.fsync();
					} catch (IOException e) {
						System.err.println("IOException");
					}

					//release the recource
					resourceMap.remove(key);
				}
			}
		} else if (type.equals("INPROGRESS")) {
			//do nothing, just wait.
		} else {
			System.err.println("deliverMessage: unrecognizable, forward to PL.getMessage()");
			return false;
		}
		return true;
	}
	
	//get all the reference numbers on which there's a recourse locked
	public Set<Integer> getAllRefs() {
		Collection<Integer> values = resourceMap.values();
		Set<Integer> uniqueRefs = new HashSet<>(values);
		return uniqueRefs;
	}

	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		UN.PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		
		//first recover the UserNode
		UN.userRecover();
		//release the flag
		UN.inRecovery = false;

		//for all the resources that have been locked, the UserNode need to check-in with the Server to see the result, in case of network faliure
		long curTime = System.currentTimeMillis();
		while (true) {
			//wait for messages to come

			//every 2s, check with the Server to see if any decision has come out
			long noW = System.currentTimeMillis();
			if (noW - curTime > 2000) {
				curTime = noW;
				Set<Integer> refs =  UN.getAllRefs();
				for (int ref : refs) {
					UN.asK(ref);
				}
			}
		}
	}
}