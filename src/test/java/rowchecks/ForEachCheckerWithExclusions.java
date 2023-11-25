package rowchecks;

import java.util.ArrayList;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;

import rowchecks.IRowCheck.CheckResult;

class ForEachCheckerWithExclusion implements ForeachFunction<Row>
{

        static ArrayList<IRowCheck> rowChecks;
        static CheckResult excludedResult;
        static ArrayList<Row> excludedRows;
        public ForEachCheckerWithExclusion(ArrayList<IRowCheck> rowChecks, CheckResult excludedResult)
        {
            ForEachCheckerWithExclusion.rowChecks = rowChecks;
            ForEachCheckerWithExclusion.excludedResult = excludedResult;
            ForEachCheckerWithExclusion.excludedRows = new ArrayList<Row>();
        }

        public void call(Row row) throws Exception {
            for (IRowCheck c : rowChecks)
            {
                if (c.check(row) == excludedResult)
                {
                    excludedRows.add(row);
                }
            } 
        } 

        public ArrayList<Row> getExclusions() { return excludedRows; }
    }  
