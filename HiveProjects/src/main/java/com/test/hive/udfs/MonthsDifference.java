package com.test.hive.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.joda.time.PeriodType;


@Description(
	name="MonthsDifference",
	value="retruns months difference, if you give startdate and enddate it returns months beetween the 2 dates",
	extended="SELECT months_diff(startDate, EndDate);")

public class MonthsDifference extends UDF {
	
	public IntWritable evaluate(String startDate, String endDate) {
		LocalDate date1 = new LocalDate(startDate); // 1996-10-30
        LocalDate date2 = new LocalDate(endDate); // 1997-02-28
        PeriodType monthDay = PeriodType.months();
        Period difference = new Period(date1, date2, monthDay);
        int months = difference.getMonths();
		
        return new IntWritable(months);
		
	}

}
