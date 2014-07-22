package com.conveyal.otpac;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

public class Util {
	public static void saveFile(File file, InputStream inputStream, long size, boolean verbose) throws IOException {
		OutputStream outputStream = null;

		try {

			// write the inputStream to a FileOutputStream
			file.getParentFile().mkdirs();
			file.createNewFile();
			outputStream = new FileOutputStream(file);

			int read = 0;
			byte[] bytes = new byte[1024];
			int totalRead = 0;

			while ((read = inputStream.read(bytes)) != -1) {
				outputStream.write(bytes, 0, read);
				totalRead += read;
				if (verbose) {
					System.out.print("\r" + totalRead + "/" + size);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
			if (outputStream != null) {
				// outputStream.flush();
				outputStream.close();

			}

			if (verbose) {
				System.out.print("\n");
			}
		}
	}
	
	public static void saveFile(String filename, InputStream inputStream, long size, boolean verbose) throws IOException {
		saveFile( new File(filename), inputStream, size, verbose );
	}
	
	
	public static String hashString(String input)  {
		
		try {
		
			byte[] bytesOfMessage = input.getBytes("UTF-8");	
			
			return DigestUtils.md5Hex(bytesOfMessage);
			
		}
		catch(Exception e) {
			
			return "";
		}
	}
	
	public static String hashFile(File file)  {
		
		try {
			
			MessageDigest md = MessageDigest.getInstance("MD5");
			
			FileInputStream fis = new FileInputStream(file);
			
			DigestInputStream dis = new DigestInputStream(fis, md);
			
			
			// hash the size
			dis.read(ByteBuffer.allocate(8).putLong(file.length()).array());
			
			
			// hash first 1000 bytes
			int i = 0;
			while (dis.read() != -1 && i < 1000) {
				i++;
			};
			
			// hash  5000 bytes starting in the middle or the remainder of the file if under 10000
			if(file.length() > 10000) {
				dis.skip(file.length() / 2);
				
				i = 0;
				while (dis.read() != -1 && i < 5000) {
					i++;
				};
			}
			else {
				while (dis.read() != -1) {
				};
			}
				
			dis.close();
			
			return new String(Hex.encodeHex(md.digest()));
			
			
		}
		catch(Exception e) {
			
			return "";
		}
	}
}
