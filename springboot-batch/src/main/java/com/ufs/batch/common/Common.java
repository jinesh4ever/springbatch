package com.ufs.batch.common;
/**
 * @author Jinesh George
 */
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.CharacterIterator;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import org.springframework.ui.Model;

//import javax.mail.internet.AddressException;
//import javax.mail.internet.InternetAddress;

//import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

//import com.tuskegee.app.controller.AdminController;

public class Common {
	private static final Logger logger = Logger.getLogger(Common.class);
	public static java.sql.Date getFormatedSqlDate(String strDate) {

		java.util.Date dtUtilDate = new java.util.Date();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
		SimpleDateFormat sdfAct = new SimpleDateFormat("dd-mm-yyyy");
		java.sql.Date sqlDate = null;

		try {
			if (strDate != null && !strDate.trim().equals("")) {

				dtUtilDate = sdfAct.parse(strDate);

				sqlDate = java.sql.Date.valueOf(sdf.format(dtUtilDate));
				// System.out.println("Date while parsing GIVEN DATE  :  "+strDate
				// +"   FORMATED DATE  :  "+sqlDate);
			} else {
				System.out
						.println("EMPTY STRING FROM getFormatedSqlDate at CLSCOMMON ");
			}
		} catch (Exception e) {
			System.out.println("Unable to parse the date string");
			logger.info(e.getMessage());
		}
		return sqlDate;

	}
	
	public static java.sql.Timestamp getCurrentTimestamp() {
		Timestamp timedate = null;
		try {
			java.util.Date date = new java.util.Date();
			timedate = new Timestamp(date.getTime());
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		return timedate;
	}

	public static Timestamp getCurrentGMTTimestamp() {
		String timedate = null;
		java.util.Date currentTime = new java.util.Date();
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			// Give it to me in GMT time.
			sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			timedate = sdf.format(currentTime);
			System.out.println("GMT time: " + sdf.format(currentTime));
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		return Common.getTimestamp(timedate, "yyyy-MM-dd hh:mm:ss");
	}
	
	public static Date getCurrentGMTDate() {
		String timedate = null;java.sql.Date sql = null;
		java.util.Date currentTime = new java.util.Date();
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			// Give it to me in GMT time.
			sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			timedate = sdf.format(currentTime);
			System.out.println("GMT time: " + sdf.format(currentTime));
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	        java.util.Date parsed = format.parse(timedate);
	        sql = new java.sql.Date(parsed.getTime());
			
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		return sql;
	}

	public static String getFormatedStringDate(java.sql.Date sqlDate) {

		java.util.Date dtUtilDate = new java.util.Date();
		SimpleDateFormat sdfAct = new SimpleDateFormat("dd-MM-yyyy");

		String strDate = null;

		try {

			if (sqlDate != null) {
				dtUtilDate = sqlDate;
				strDate = sdfAct.format(dtUtilDate);

				// System.out.println("Date while parsing GIVEN DATE  :  "+strDate
				// +"   FORMATED DATE  :  "+sqlDate);
			}
		} catch (Exception e) {
			System.out.println("Unable to parse the date string");
			logger.info(e.getMessage());
		}
		return strDate;

	}

	public static java.sql.Date getCurrentDate() { // //////For getting current
													// date

		String dtDate = null;
		java.sql.Date sqlDate = null;
		try {

			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");// Getting
																		// Current
																		// Date
			java.util.Date date = new java.util.Date();
			dtDate = dateFormat.format(date);
			sqlDate = java.sql.Date.valueOf(dtDate);
		} catch (Exception e) {
			System.out.println("Unable to parse the date--- string");
			logger.info(e.getMessage());
		}
		return sqlDate;

	}

	public static String getMonthName(int nmonth) {
		String retValue = "";
		String[] months = { "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY",
				"JUNE", "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER",
				"DECEMBER" };

		retValue = months[nmonth - 1];
		return retValue;
	}

	static public java.sql.Date getMonthEndDate(int month, int year) {

		int tday = 0;
		java.sql.Date toDate = null;

		try {
			if (month == 1 || month == 3 || month == 5 || month == 7
					|| month == 8 || month == 10 || month == 12)
				tday = 31;

			else if (month == 4 || month == 6 || month == 9 || month == 11)
				tday = 30;
			else if (month == 2) {
				if ((year % 4) == 0)
					tday = 29;
				else
					tday = 28;
			} else {
			}

			String sTDate = year
					+ "-"
					+ (String.valueOf(month).length() == 1 ? "0" + month
							: month) + "-" + tday;

			toDate = java.sql.Date.valueOf(sTDate);
		} catch (Exception e) {
			System.out
					.println("ERROR IN DATE CONVERTION ClsCommon,getMonthEndDate :"
							+ e);
			logger.info(e.getMessage());

		}

		return toDate;

	}

	static public java.sql.Date getMonthStartDate(int month, int year) {

		java.sql.Date fromDate = null;
		try {
			String sFDate = year
					+ "-"
					+ (String.valueOf(month).length() == 1 ? "0" + month
							: month) + "-" + "01";

			fromDate = java.sql.Date.valueOf(sFDate);
		} catch (Exception e) {
			System.out
					.println("ERROR IN DATE CONVERTION ClsCommon, getMonthStartDate :"
							+ e);
			logger.info(e.getMessage());

		}

		return fromDate;

	}

	public static int getDaysBetween(java.sql.Date dateOne,
			java.sql.Date dateTwo) {
		long milliseconds = dateTwo.getTime() - dateOne.getTime();
		if (milliseconds < 0) {
			milliseconds = -milliseconds;
		}

		long days = milliseconds / 1000L / 60L / 60L / 24L;
		int day = (int) days;
		return day;
	}

	public static int getNoOfDays(java.sql.Date dateOne, java.sql.Date dateTwo) {
		long milliseconds = dateTwo.getTime() - dateOne.getTime();
		if (milliseconds < 0) {
			milliseconds = -milliseconds;
		}

		long days = milliseconds / 1000L / 60L / 60L / 24L;
		int day = (int) days + 1;
		return day;
	}

	// ******************* NUMBER ROUND OFF ****************************//

	public static String formatNumber(double dbl, int iFranctionDigit) {
		NumberFormat nf = NumberFormat.getInstance();
		nf.setGroupingUsed(false);
		nf.setMaximumIntegerDigits(20);
		nf.setMinimumFractionDigits(iFranctionDigit);
		nf.setMaximumFractionDigits(iFranctionDigit);
		return nf.format(dbl);
	}

	public static String formatNumber(String str, int iFranctionDigit)
			throws NumberFormatException {
		Double dblObj = new Double(str);
		double dbl = dblObj.doubleValue();
		return formatNumber(dbl, iFranctionDigit);
	}

	public static String formatNumber(float flt, int iFranctionDigit) {
		return formatNumber((double) flt, iFranctionDigit);
	}

	/**
	 * To parse Escape characters in the text appearing as XML data, between
	 * tags.
	 **/

	public static String parseStringForXML(String aText) {
		if (aText == null)
			return " ";
		final StringBuilder result = new StringBuilder();
		final StringCharacterIterator iterator = new StringCharacterIterator(
				aText);
		char character = iterator.current();
		while (character != CharacterIterator.DONE) {

			if (character == '<') {
				result.append("&lt;");
			} else if (character == '>') {
				result.append("&gt;");
			} else if (character == '&') {
				result.append("&amp;");
			} else if (character == '\"') {
				result.append("&quot;");
			} else if (character == '\t') {
				result.append("&#009;");
			} else if (character == '!') {
				result.append("&#033;");
			} else if (character == '#') {
				result.append("&#035;");
			} else if (character == '$') {
				result.append("&#036;");
			} else if (character == '%') {
				result.append("&#037;");
			} else if (character == '\'') {
				result.append("&#039;");
			} else if (character == '(') {
				result.append("&#040;");
			} else if (character == ')') {
				result.append("&#041;");
			} else if (character == '*') {
				result.append("&#042;");
			} else if (character == '+') {
				result.append("&#043;");
			} else if (character == ',') {
				result.append("&#044;");
			} else if (character == '-') {
				result.append("&#045;");
			} else if (character == '.') {
				result.append("&#046;");
			} else if (character == '/') {
				result.append("&#047;");
			} else if (character == ':') {
				result.append("&#058;");
			} else if (character == ';') {
				result.append("&#059;");
			} else if (character == '=') {
				result.append("&#061;");
			} else if (character == '?') {
				result.append("&#063;");
			} else if (character == '@') {
				result.append("&#064;");
			} else if (character == '[') {
				result.append("&#091;");
			} else if (character == '\\') {
				result.append("&#092;");
			} else if (character == ']') {
				result.append("&#093;");
			} else if (character == '^') {
				result.append("&#094;");
			} else if (character == '_') {
				result.append("&#095;");
			} else if (character == '`') {
				result.append("&#096;");
			} else if (character == '{') {
				result.append("&#123;");
			} else if (character == '|') {
				result.append("&#124;");
			} else if (character == '}') {
				result.append("&#125;");
			} else if (character == '~') {
				result.append("&#126;");
			} else {// the char is not a special one //add it to the result as
					// is
				result.append(character);
			}
			character = iterator.next();
		}
		return result.toString();
	}

	static public java.sql.Timestamp getTimeStamp(String strDate, String time,
			String amPm) {

		java.sql.Timestamp timestamp = null;
		try {

			String[] timeArray = time.split(":");

			int intTime = Integer.parseInt(timeArray[0].trim());
			// if(amPm.equalsIgnoreCase("PM"))
			// {
			//
			// if(intTime==12)
			// {
			// intTime =12;
			// }
			// else
			// {
			// intTime = intTime +12;
			// }
			// }

			String strTime = intTime + "" + ":" + timeArray[1].trim() + ":"
					+ "00" + " " + amPm;

			strDate = strDate + " " + strTime;
			SimpleDateFormat dateFormat = new SimpleDateFormat(
					"dd-MM-yyyy hh:mm:ss a");
			java.util.Date parsedDate = dateFormat.parse(strDate);

			timestamp = new java.sql.Timestamp(parsedDate.getTime());

		} catch (Exception e) {
			System.out.println("ERROR IN getTimeStamp ClsCommon :" + e);
			logger.info(e.getMessage());

		}

		return timestamp;

	}

	static public ArrayList<String> getDateAndTime(java.sql.Timestamp timeStamp) {
		ArrayList<String> dateList = new ArrayList<String>();

		try {
			String strTimeStamp = timeStamp + "";
			String[] strTimeStampArray = strTimeStamp.split(" ");
			String[] timeArray = (strTimeStampArray[1]).split(":");
			int hours = 0;
			String time = "";
			String amPm = "";

			if (Integer.parseInt(timeArray[0]) >= 12) {
				hours = Integer.parseInt(timeArray[0]) - 12;
				if (timeArray[0].equals("12")) {
					time = 12 + "" + ":" + timeArray[1];
					amPm = "PM";
				} else {
					time = hours + "" + ":" + timeArray[1];
					amPm = "PM";
				}
			} else {
				hours = Integer.parseInt(timeArray[0]);
				time = hours + "" + ":" + timeArray[1];
				amPm = "AM";
			}

			String strDate = "";
			if (strTimeStampArray[0] != null) {
				String[] dayArray = strTimeStampArray[0].split("-");
				strDate = dayArray[2] + "-" + dayArray[1] + "-" + dayArray[0];

			}

			dateList.add(strDate);
			dateList.add(time);
			dateList.add(amPm);

		} catch (Exception e) {
			System.out.println("ERROR IN getDateAndTime ClsCommon :" + e);

		}

		return dateList;

	}

	/** Function to change Currency to Words ----START------ */

	public static String getCurrencyToWords(String numb) {
		return changeToWords(numb, true);
	}

	private static String changeToWords(String numb, boolean isCurrency) {
		System.out.println("Eneterd method");
		String val = "", wholeNo = numb, points = "", andStr = "", pointStr = "";
		String endStr = (isCurrency) ? (" Only") : ("");
		try {

			int decimalPlace = numb.indexOf(".");
			System.out.println("DECIMALPL:ACES ARE:" + decimalPlace);
			if (decimalPlace > 0) {
				wholeNo = numb.substring(0, decimalPlace);
				points = numb.substring(decimalPlace + 1);
				if (Integer.parseInt(points) > 0) {
					if (points.length() > 2) {
						points = points.substring(0, 2);
					}
					andStr = (isCurrency) ? (" Rupees and ") : ("point");// just
																			// to
																			// separate
																			// whole
																			// numbers
																			// from
																			// points/cents

					endStr = (isCurrency) ? (" Paise " + endStr) : ("");

					// pointStr =
					// translateWholeNumber(points);//translateCents(points);
					pointStr = convert(Long.valueOf(points));
				}

			}
			val = convert(Long.valueOf(wholeNo)) + andStr + pointStr + endStr;
		}

		catch (Exception e) {
		}
		return val;
	}

	private static final String[] tensNames = { "", " ten", " twenty",
			" thirty", " forty", " fifty", " sixty", " seventy", " eighty",
			" ninety" };

	private static final String[] numNames = { "", " one", " two", " three",
			" four", " five", " six", " seven", " eight", " nine", " ten",
			" eleven", " twelve", " thirteen", " fourteen", " fifteen",
			" sixteen", " seventeen", " eighteen", " nineteen" };

	private static String convertLessThanOneThousand(int number) {
		String soFar;

		if (number % 100 < 20) {
			soFar = numNames[number % 100];
			number /= 100;
		} else {
			soFar = numNames[number % 10];
			number /= 10;

			soFar = tensNames[number % 10] + soFar;
			number /= 10;
		}
		if (number == 0)
			return soFar;
		return numNames[number] + " hundred" + soFar;
	}

	private static String convert(long number) {
		// 0 to 999 999 999 999
		if (number == 0) {
			return "zero";
		}

		String snumber = Long.toString(number);

		// pad with "0"
		String mask = "000000000000";
		DecimalFormat df = new DecimalFormat(mask);
		snumber = df.format(number);

		// XXXnnnnnnnnn
		int billions = Integer.parseInt(snumber.substring(0, 3));
		// nnnXXXnnnnnn
		int millions = Integer.parseInt(snumber.substring(3, 6));
		// nnnnnnXXXnnn
		int hundredThousands = Integer.parseInt(snumber.substring(6, 9));
		// nnnnnnnnnXXX
		int thousands = Integer.parseInt(snumber.substring(9, 12));

		String tradBillions;
		switch (billions) {
		case 0:
			tradBillions = "";
			break;
		case 1:
			tradBillions = convertLessThanOneThousand(billions) + " billion ";
			break;
		default:
			tradBillions = convertLessThanOneThousand(billions) + " billion ";
		}
		String result = tradBillions;

		String tradMillions;
		switch (millions) {
		case 0:
			tradMillions = "";
			break;
		case 1:
			tradMillions = convertLessThanOneThousand(millions) + " million ";
			break;
		default:
			tradMillions = convertLessThanOneThousand(millions) + " million ";
		}
		result = result + tradMillions;

		String tradHundredThousands;
		switch (hundredThousands) {
		case 0:
			tradHundredThousands = "";
			break;
		case 1:
			tradHundredThousands = "one thousand ";
			break;
		default:
			tradHundredThousands = convertLessThanOneThousand(hundredThousands)
					+ " thousand ";
		}
		result = result + tradHundredThousands;

		String tradThousand;
		tradThousand = convertLessThanOneThousand(thousands);
		result = result + tradThousand;
		// remove extra spaces!
		return result.replaceAll("^\\s+", "").replaceAll("\\b\\s{2,}\\b", " ");
	}

	/** Function to change Currency to Words ----END------ */

	public static String getDateInWords(java.sql.Date argDate) {
		String date[] = Common.getFormatedStringDate(argDate).split("-");
		int day = Integer.parseInt(date[0]);
		int month = Integer.parseInt(date[1]);
		int year = Integer.parseInt(date[2]);

		String sDay = "";
		String sMonth = "";
		String sYear = "";

		sDay = convertLessThanOneThousand(day);
		sMonth = getMonthName(month);
		sYear = convert(year);

		sDay = sDay.replace("one", "first");
		sDay = sDay.replace("two", "second");
		sDay = sDay.replace("three", "third");
		sDay = sDay.replace("four", "fourth");
		sDay = sDay.replace("five", "fifth");
		sDay = sDay.replace("six", "sixth");
		sDay = sDay.replace("seven", "seventh");
		sDay = sDay.replace("eight", "eighth");
		sDay = sDay.replace("nine", "nineth");
		sDay = sDay.replace("ten", "tenth");
		sDay = sDay.replace("eleven", "eleventh");
		sDay = sDay.replace("twelve", "twelveth");
		sDay = sDay.replace("teen", "teenth");

		String DATE = sDay + " " + sMonth + ", " + sYear;
		DATE = DATE.replace("one thousand nine hundred", "nineteen");
		DATE = convFirstLetterUpper(DATE.trim());

		return DATE;
	}

	public static String getDateWithMonthInWords(String argDate) {
		String date[] = argDate.split("-");
		String sDay = date[0];

		String day = "";
		day = sDay.substring(0, 1);

		if (day.equals("0")) {

			day = sDay.substring(1);

			if (day.equals("1")) {
				day = sDay.substring(1) + "st";
			} else if (day.equals("2")) {
				day = sDay.substring(1) + "nd";

			} else if (day.equals("3")) {
				day = sDay.substring(1) + "rd";
			} else if (day.equals("4") || day.equals("5") || day.equals("6")
					|| day.equals("7") || day.equals("8") || day.equals("9")) {
				day = sDay.substring(1) + "th";
			}
		} else {
			day = date[0];
			if (day.equals("10") || day.equals("11") || day.equals("12")
					|| day.equals("13") || day.equals("14") || day.equals("15")
					|| day.equals("16") || day.equals("17") || day.equals("18")
					|| day.equals("19") || day.equals("20")) {
				day = sDay + "th";
			} else if (day.equals("21") || day.equals("31")) {
				day = sDay + "st";
			} else if (day.equals("22")) {
				day = sDay + "nd";
			} else if (day.equals("23")) {
				day = sDay + "rd";
			} else if (day.equals("24") || day.equals("25") || day.equals("26")
					|| day.equals("27") || day.equals("28") || day.equals("29")
					|| day.equals("30")) {
				day = sDay + "th";
			}
		}

		int month = Integer.parseInt(date[1]);
		String sYear = date[2];

		String sMonth = "";

		sMonth = getMonthName(month);
		String DATE = day + " " + convFirstLetterUpper(sMonth.trim()) + ", "
				+ sYear;

		return DATE;
	}

	public static String getMonthANDYearInWords(String argDate) {
		String date[] = argDate.split("-");
		String sDay = date[0];

		String day = "";
		day = sDay.substring(0, 1);

		if (day.equals("0")) {

			day = sDay.substring(1);

			if (day.equals("1")) {
				day = sDay.substring(1) + "st";
			} else if (day.equals("2")) {
				day = sDay.substring(1) + "nd";

			} else if (day.equals("3")) {
				day = sDay.substring(1) + "rd";
			} else if (day.equals("4") || day.equals("5") || day.equals("6")
					|| day.equals("7") || day.equals("8") || day.equals("9")) {
				day = sDay.substring(1) + "th";
			}
		} else {
			day = date[0];
			if (day.equals("10") || day.equals("11") || day.equals("12")
					|| day.equals("13") || day.equals("14") || day.equals("15")
					|| day.equals("16") || day.equals("17") || day.equals("18")
					|| day.equals("19") || day.equals("20")) {
				day = sDay + "th";
			} else if (day.equals("21") || day.equals("31")) {
				day = sDay + "st";
			} else if (day.equals("22")) {
				day = sDay + "nd";
			} else if (day.equals("23")) {
				day = sDay + "rd";
			} else if (day.equals("24") || day.equals("25") || day.equals("26")
					|| day.equals("27") || day.equals("28") || day.equals("29")
					|| day.equals("30")) {
				day = sDay + "th";
			}
		}

		int month = Integer.parseInt(date[1]);
		String sYear = date[2];

		String sMonth = "";

		sMonth = getMonthName(month);
		String DATE = convFirstLetterUpper(sMonth.trim()) + ", " + sYear;

		return DATE;
	}

	public static java.sql.Date getDate(java.sql.Date argDate, int period,
			char periodtype) {
		String basedate = argDate.toString();
		String retDate = "";
		try {
			java.util.GregorianCalendar c = new java.util.GregorianCalendar();
			int sDates[] = { 0, 0, 0 };
			int temp[] = { 0, 0, 0 };

			if (basedate.length() > 7) {
				if (basedate.charAt(4) == '-' && basedate.charAt(7) == '-') {

					Pattern pat = Pattern.compile("\\-");
					String tmp[] = pat.split(basedate);
					sDates[0] = Integer.valueOf(tmp[0]).intValue();
					sDates[1] = Integer.valueOf(tmp[1]).intValue();
					sDates[2] = Integer.valueOf(tmp[2]).intValue();
					c.set(sDates[0], sDates[1] - 1, sDates[2], 0, 0);

					switch (periodtype) {
					case 'D':
						c.add(GregorianCalendar.DAY_OF_MONTH, period);
						break;
					case 'M':
						c.add(GregorianCalendar.MONTH, period);
						break;
					case 'Y':
						c.add(GregorianCalendar.YEAR, period);
						break;
					case 'W':
						c.add(GregorianCalendar.WEEK_OF_YEAR, period);
						break;
					}

					temp[0] = c.get(GregorianCalendar.YEAR);
					temp[1] = c.get(GregorianCalendar.MONTH);
					temp[2] = c.get(GregorianCalendar.DAY_OF_MONTH);

					retDate = String.valueOf(temp[0])
							+ "-"
							+ (temp[1] < 9 ? "0"
									+ (String.valueOf((temp[1] + 1))) : String
									.valueOf((temp[1] + 1)))
							+ "-"
							+ (temp[2] < 10 ? "0" + String.valueOf(temp[2])
									: String.valueOf(temp[2]));
				}
			}
		} catch (Exception e) {

			System.out.println("Error getDate  " + e);
		}
		return java.sql.Date.valueOf(retDate);
	}

	public static Timestamp getPlusDateTime(java.sql.Date argDate, int period,
			char periodtype, Timestamp startdate) {
		System.out.println("argDate ==="+argDate);
	
		String basedate = argDate.toString();
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		String dttime = dateFormat.format(startdate);
		String[] tmptime = dttime.split(":");

		String retDate = "";
		Timestamp dt = null;
		try {
			java.util.GregorianCalendar c = new java.util.GregorianCalendar();
			int sDates[] = { 0, 0, 0 };
			int temp[] = { 0, 0, 0 };

			if (basedate.length() > 7) {
				if (basedate.charAt(4) == '-' && basedate.charAt(7) == '-') {

					Pattern pat = Pattern.compile("\\-");
					String tmp[] = pat.split(basedate);
					sDates[0] = Integer.valueOf(tmp[0]).intValue();
					sDates[1] = Integer.valueOf(tmp[1]).intValue();
					sDates[2] = Integer.valueOf(tmp[2]).intValue();
					c.set(sDates[0], sDates[1] - 1, sDates[2], 0, 0);

					switch (periodtype) {
					case 'D':
						c.add(GregorianCalendar.DAY_OF_MONTH, period);
						break;
					case 'M':
						c.add(GregorianCalendar.MONTH, period);
						break;
					case 'Y':
						c.add(GregorianCalendar.YEAR, period);
						break;
					case 'W':
						c.add(GregorianCalendar.WEEK_OF_YEAR, period);
						break;
					}

					temp[0] = c.get(GregorianCalendar.YEAR);
					temp[1] = c.get(GregorianCalendar.MONTH);
					temp[2] = c.get(GregorianCalendar.DAY_OF_MONTH);
					String timeD = String.valueOf(tmptime[0]) + ":"
							+ String.valueOf(tmptime[1]) + ":"
							+ String.valueOf(tmptime[2]);
					retDate = String.valueOf(temp[0])
							+ "-"
							+ (temp[1] < 9 ? "0"
									+ (String.valueOf((temp[1] + 1))) : String
									.valueOf((temp[1] + 1)))
							+ "-"
							+ (temp[2] < 10 ? "0" + String.valueOf(temp[2])
									: String.valueOf(temp[2]) + " ");
					String tm = retDate.trim() + " " + timeD.trim();
					System.out.println("tmmm====="+tm);
					dt = java.sql.Timestamp.valueOf(tm);

				}
			}
		} catch (Exception e) {

			System.out.println("Error getDate  " + e);
			logger.info(e.getMessage());
		}

		return dt;
	}

	public static String getFullCurrentTime() {
		Calendar calendar = new GregorianCalendar();
		String am_pm;
		int hour = calendar.get(Calendar.HOUR);
		int minute = calendar.get(Calendar.MINUTE);
		int second = calendar.get(Calendar.SECOND);
		if (calendar.get(Calendar.AM_PM) == 0)
			am_pm = "AM";
		else
			am_pm = "PM";
		if (hour == 0)
			hour = 12;
		return hour + ":" + minute + ":" + second + " " + am_pm;
	}

	public static String getCurrentTime() {
		Calendar calendar = new GregorianCalendar();
		String am_pm;
		int hour = calendar.get(Calendar.HOUR);
		int minute = calendar.get(Calendar.MINUTE);
		if (calendar.get(Calendar.AM_PM) == 0)
			am_pm = "AM";
		else
			am_pm = "PM";
		if (hour == 0)
			hour = 12;
		return hour + ":" + minute + " " + am_pm;
	}

	// public static String convFirstLetterUpper(String sentence)
	// {
	//
	// String converted="";
	// char first;
	// int word_length;
	//
	// sentence=sentence.toLowerCase();
	//
	// String[] words = sentence.split(" ");
	//
	// for (String word : words)
	// {
	// first=Character.toUpperCase(word.charAt(0));
	// word_length=word.length();
	// word=first+word.substring(1,word_length);
	// //word=word.replace(word.charAt(0),first);
	// converted=converted+" "+word;
	// }
	// return converted;
	//
	// }

	public static String convFirstLetterUpper(String sentence) {

		String converted = "";
		char first;
		int word_length;

		if (sentence == null) {
			return "";
		}
		sentence = sentence.toLowerCase();

		StringTokenizer tk = new StringTokenizer(sentence, " ");
		while (tk.hasMoreElements()) {
			String word = tk.nextToken().toString();
			first = Character.toUpperCase(word.charAt(0));
			word_length = word.length();
			word = first + word.substring(1, word_length);
			converted = converted + " " + word;
		}
		return converted;
	}

	public static String getFirstLetterCaps(String name) {
		String AfterReplace = null;
		String finalName = "";
		if (name.contains(".")) {
			StringTokenizer subFname = new StringTokenizer(name, ".");
			int k = subFname.countTokens();
			System.out.println("count-------" + k);
			for (int i = 0; i < k; i++) {
				AfterReplace = subFname.nextToken().toLowerCase();
				AfterReplace = AfterReplace.replaceFirst(AfterReplace
						.substring(0, 1),
						AfterReplace.toUpperCase().substring(0, 1));
				finalName = finalName + AfterReplace;
				if (i != k - 1)
					finalName = finalName + ".";
			}
			return finalName;
		} else {
			AfterReplace = name.toLowerCase();
			AfterReplace = AfterReplace.replaceFirst(AfterReplace.substring(0,
					1), AfterReplace.toUpperCase().substring(0, 1));
			return AfterReplace;
		}

	}

	public static String getAbbreviateName(String fulName) {
		String AfterReplace[];
		String finalName = "";
		String temp;

		int i = 0, j = 0, k = 0;

		try {
			if (fulName.contains(" ")) {
				AfterReplace = fulName.split(" ");
				StringTokenizer fName = new StringTokenizer(AfterReplace[0],
						".");
				k = fName.countTokens();

				for (i = 0; i < k; i++) {
					temp = fName.nextToken();
					temp = temp.toLowerCase();
					temp = temp.replaceFirst(temp.substring(0, 1), temp
							.toUpperCase().substring(0, 1));
					finalName = finalName + temp;
					if (i != k - 1)
						finalName = finalName + ".";
				}

				if (AfterReplace.length > 3)
					j = 3;
				else
					j = AfterReplace.length;

				for (i = 0; i < j - 1; i++)
					finalName = finalName + " "
							+ AfterReplace[i + 1].toUpperCase().substring(0, 1);

				return finalName;

			} else {
				return fulName;
			}
		} catch (StringIndexOutOfBoundsException e) {
			return fulName;
		}

	}

	public static String getDayNameFrmDate(String date, Connection conn) {
		String day = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn
					.prepareStatement("SELECT TO_CHAR(?,'DAY') AS DAY FROM DUAL");
			ps.setDate(1, getFormatedSqlDate(date));
			rs = ps.executeQuery();
			if (rs.next()) {
				day = rs.getString("DAY");
			}

		} catch (Exception e) {
			System.out.println("ERROR IN CLS COMMON ...getDayNameFrmDate..."
					+ e);
		}

		return day;
	}

	public static int getYearFromStringDate(String DtDate) {
		int year = 0;
		String[] dateArray = null;

		dateArray = DtDate.split("-");
		year = Integer.parseInt(dateArray[2]);

		return year;
	}

	public static float roundOfToNextTen(float value) {
		if ((value % 10) > 0) {
			value = value + (10 - (value % 10));
			return value;
		} else
			return value;

	}
/*
	public static String encoder(String param)
			throws UnsupportedEncodingException {
		byte[] binaryData = param.getBytes("UTF8");
		decoder(new String(Base64.encodeBase64(binaryData)));
		return new String(Base64.encodeBase64(binaryData));
	}

	public static String decoder(String encoded)
			throws UnsupportedEncodingException {
		byte[] encodedBytes = encoded.getBytes("UTF-8");
		byte[] decodeBase64 = Base64.decodeBase64(encodedBytes);
		return new String(decodeBase64);
	}
*/
	public static Timestamp getTimestamp(String trackeddatetime) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
		java.util.Date parsedDate = null;
		try {
			parsedDate = sdf.parse(trackeddatetime);
		} catch (ParseException e) {
			logger.info(e.getMessage());
		}
		return new java.sql.Timestamp(parsedDate.getTime());
	}

	public static Timestamp getTimestamp(String trackeddatetime, String toFormat) {
		SimpleDateFormat sdf = new SimpleDateFormat(toFormat);
		java.util.Date parsedDate = null;
		try {
			parsedDate = sdf.parse(trackeddatetime);
		} catch (ParseException e) {
			logger.info(e.getMessage());
		}
		return new java.sql.Timestamp(parsedDate.getTime());
	}

	public static Time getTime(String trackedTime) {
		DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
		Time timeValue = null;
		try {
			timeValue = new java.sql.Time(formatter.parse(trackedTime)
					.getTime());
		} catch (ParseException e) {
			logger.info(e.getMessage());
		}
		return timeValue;
	}

	public static Date getDateFromTimestamp(String trackeddatetime) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
		java.util.Date parsedDate = null;
		try {
			parsedDate = sdf.parse(trackeddatetime);
		} catch (ParseException e) {
			logger.info(e.getMessage());
		}
		return new java.sql.Date(parsedDate.getTime());
	}

	public static Time getTimeFromTimestamp(String trackeddatetime) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
		java.util.Date parsedDate = null;
		try {
			parsedDate = sdf.parse(trackeddatetime);
		} catch (ParseException e) {
			logger.info(e.getMessage());
		}
		return new java.sql.Time(parsedDate.getTime());
	}

	public static Time subtractTimeBuffer(Time trackedTime, Time timeBuffer) {
		java.util.Date reqEndtimekk = null;
		SimpleDateFormat sdfkk = null;
		Calendar instance = null;
		try {

			String aa = timeBuffer + "";
			int h = Integer.parseInt(aa.split(":")[0]);
			int m = Integer.parseInt(aa.split(":")[1]);
			int s = Integer.parseInt(aa.split(":")[2]);
			String allexam_end_time = trackedTime + "";
			sdfkk = new SimpleDateFormat("HH:mm:ss");
			reqEndtimekk = sdfkk.parse(allexam_end_time);
			instance = GregorianCalendar.getInstance();
			instance.setTime(reqEndtimekk);
			instance.add(GregorianCalendar.MINUTE, -m);
			instance.add(GregorianCalendar.HOUR, -h);
			instance.add(GregorianCalendar.SECOND, -s);
		} catch (Exception e) {
			logger.info(e.getMessage());
		}

		return new Time(instance.getTime().getTime());

	}

	public static Time addTimeBuffer(Time trackedTime, Time timeBuffer) {
		java.util.Date reqEndtimekk = null;
		SimpleDateFormat sdfkk = null;
		Calendar instance = null;
		try {
			String aa = timeBuffer + "";
			int h = Integer.parseInt(aa.split(":")[0]);
			int m = Integer.parseInt(aa.split(":")[1]);
			int s = Integer.parseInt(aa.split(":")[2]);
			String allexam_end_time = trackedTime + "";
			sdfkk = new SimpleDateFormat("HH:mm:ss");
			reqEndtimekk = sdfkk.parse(allexam_end_time);
			instance = GregorianCalendar.getInstance();
			instance.setTime(reqEndtimekk);
			instance.add(GregorianCalendar.MINUTE, m);
			instance.add(GregorianCalendar.HOUR, h);
			instance.add(GregorianCalendar.SECOND, s);
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		return new Time(instance.getTime().getTime());
	}
	
	public static String encrypt(String str) {

		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			logger.info(e.getMessage());
		}
		md.update(str.getBytes());

		byte byteData[] = md.digest();

		// convert the byte to hex format
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < byteData.length; i++) {
			sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
		}		

		return sb.toString();

	}
	
	public static String getSaltString() {
        String SALTCHARS = "abcdefghijkmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 8) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
        
    }

	public static String toCamelCase(final String init) {
	    if (init==null)
	        return null;

	    final StringBuilder ret = new StringBuilder(init.length());

	    for (final String word : init.split(" ")) {
	        if (!word.isEmpty()) {
	            ret.append(word.substring(0, 1).toUpperCase());
	            ret.append(word.substring(1).toLowerCase());
	        }
	        if (!(ret.length()==init.length()))
	            ret.append(" ");
	    }

	    return ret.toString();
	}
	
	public static boolean checkDateFormat(final Date dateTemp){
		String value = getFormatedStringDate(dateTemp);
		java.util.Date date = null;
		
		try {
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
		    date = sdf.parse(value);
		    if (!value.equals(sdf.format(date))) {
		        date = null;
		    }
		} catch (ParseException ex) {
		   logger.error(ex.getMessage());
		}
		if (date == null) {
		    return false;
		} else {
		    return true;
		}
	}

	public static Model addReferrer(Model model, HttpServletRequest req) {
		String referrer = req.getHeader("referer");
		if(referrer!=null && referrer.trim().length()>0) {
			model.addAttribute("referrer", referrer);
		}else {
			model.addAttribute("referrer", "#");
		}
		return model;
	}
	
	static public String getDateAndTimeString(java.sql.Timestamp timeStamp) {
		ArrayList<String> dateList = new ArrayList<String>();

		try {
			String strTimeStamp = timeStamp + "";
			String[] strTimeStampArray = strTimeStamp.split(" ");
			String[] timeArray = (strTimeStampArray[1]).split(":");
			int hours = 0;
			String time = "";
			String amPm = "";

			if (Integer.parseInt(timeArray[0]) >= 12) {
				hours = Integer.parseInt(timeArray[0]) - 12;
				if (timeArray[0].equals("12")) {
					time = 12 + "" + ":" + timeArray[1];
					amPm = "PM";
				} else {
					time = hours + "" + ":" + timeArray[1];
					amPm = "PM";
				}
			} else {
				hours = Integer.parseInt(timeArray[0]);
				time = hours + "" + ":" + timeArray[1];
				amPm = "AM";
			}

			String strDate = "";
			if (strTimeStampArray[0] != null) {
				String[] dayArray = strTimeStampArray[0].split("-");
				strDate = dayArray[2] + "-" + dayArray[1] + "-" + dayArray[0];

			}

			dateList.add(strDate);
			dateList.add(time);
			dateList.add(amPm);

		} catch (Exception e) {
			System.out.println("ERROR IN getDateAndTime ClsCommon :" + e);

		}

		return dateList.get(0)+" "+dateList.get(1)+" "+dateList.get(2) ;

	}
	

}
