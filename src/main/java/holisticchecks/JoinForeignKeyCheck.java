package holisticchecks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

@Deprecated
public class JoinForeignKeyCheck {
    
    String targetColumn;
    String isForeignKeyColumn;
    Dataset<Row> foreignKeyDataset;

    public JoinForeignKeyCheck(String targetColumn, Dataset<Row> foreignKeyDataset, String foreignKeyColumn)
    {
        this.targetColumn = targetColumn;
        this.isForeignKeyColumn = foreignKeyColumn.equals("isFK") ? "$isFK" : "isFK";
        this.foreignKeyDataset = foreignKeyDataset.select(foreignKeyColumn).withColumnRenamed(foreignKeyColumn, targetColumn)
                                                .withColumn(isForeignKeyColumn, functions.lit(1));      
    }

    public Dataset<Row> check(Dataset<Row> df)
    {
        return df.join(foreignKeyDataset, targetColumn, "left").where(isForeignKeyColumn + " IS NULL");
    }
}
