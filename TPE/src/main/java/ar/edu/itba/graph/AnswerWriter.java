package ar.edu.itba.graph;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class AnswerWriter {

    
    private final Path parentPath;
    private final FileSystem fs;
    
    public AnswerWriter(final Path parentPath, final FileSystem fs) {
        this.parentPath = parentPath;
        this.fs = fs;
    }

    private BufferedWriter getBufferedWriter(final String filePath) throws IOException {
		final Path outPath = new Path(parentPath, filePath);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		return new BufferedWriter(new OutputStreamWriter(fs.create(outPath), StandardCharsets.UTF_8.name()));
	}

    public void writeAnswer(final Dataset<Row> ans, final Function<Row, String> rowToString, final String header, final String filePath) throws IOException {
        
        final BufferedWriter writter = getBufferedWriter(filePath);

        if(header != null) {
            writter.write(header);
            writter.newLine();
            writter.newLine();
        }
        
        for(final Row row : ans.collectAsList()) {
            writter.write(rowToString.apply(row));
            writter.newLine();
        }
        writter.close();
    }
}
