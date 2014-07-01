package com.conveyal.otpac;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
}
