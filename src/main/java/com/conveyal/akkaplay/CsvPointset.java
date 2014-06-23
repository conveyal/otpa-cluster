package com.conveyal.akkaplay;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvPointset implements Pointset, Serializable {

	private static final long serialVersionUID = -3751514874782025981L;
	private List<Point> points;

	CsvPointset() {
		points = new ArrayList<Point>();
	}

	public static Pointset fromStream(InputStream is) throws Exception {
		CsvPointset ret = new CsvPointset();

		// TODO use a real CSV parser

		BufferedReader lines = new BufferedReader(new InputStreamReader(is));

		// get header map
		String headerLine = lines.readLine();
		String[] headerFields = headerLine.split(",");
		Map<String, Integer> header = new HashMap<String, Integer>();
		for (int i = 0; i < headerFields.length; i++) {
			header.put(headerFields[i], i);
		}

		String line;
		while ((line = lines.readLine()) != null) {
			String fields[] = line.split(",");
			Point pt = createPoint(headerFields, fields);
			ret.add(pt);
		}

		return ret;
	}

	private void add(Point pt) {
		this.points.add(pt);
	}

	private static Point createPoint(String[] headerFields, String[] fields) throws Exception {
		if (headerFields.length != fields.length) {
			throw new Exception("headerFields and Fields aren't equal");
		}

		Point ret = new Point();
		for (int i = 0; i < headerFields.length; i++) {
			String headerField = headerFields[i];
			String field = fields[i];

			if (headerField.equals("lat")) {
				ret.setLat(Float.parseFloat(field));
			} else if (headerField.equals("lon")) {
				ret.setLon(Float.parseFloat(field));
			} else {
				ret.setProp(headerField, field);
			}
		}

		return ret;
	}

	@Override
	public int size() {
		return this.points.size();
	}

	@Override
	public Pointset split(int divisions, int seg) {
		CsvPointset ret = new CsvPointset();

		float seglen = this.size() / ((float) divisions);

		int start = Math.round(seglen * seg);
		int end = Math.round(seglen * (seg + 1));
		for (int i = start; i < end; i++) {
			ret.add(this.get(i));
		}

		return ret;
	}

	private Point get(int i) {
		return this.points.get(i);
	}

	@Override
	public List<Point> getPoints() {
		return this.points;
	}

}
