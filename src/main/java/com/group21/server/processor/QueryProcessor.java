package com.group21.server.processor;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.server.models.QueryType;
import com.group21.server.queries.createtable.CreateTableQueryExecutor;
import com.group21.server.queries.deletequery.DeleteQueryExecutor;
import com.group21.server.queries.droptable.DropTableQueryExecutor;
import com.group21.server.queries.insert.InsertQueryExecutor;
import com.group21.server.queries.select.SelectQueryExecutor;
import com.group21.server.queries.updatequery.UpdateQueryExecutor;

public class QueryProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryProcessor.class);

    private QueryProcessor() {
    }

    public static void process(String query) {
        query = query.toUpperCase();

        long startTime = System.currentTimeMillis();
        LOGGER.debug("Execution started for query - '{}' on {}", query, new Date(startTime));
        QueryType queryType = QueryType.from(query);

        switch (queryType) {
            case CREATE:
                CreateTableQueryExecutor executor = new CreateTableQueryExecutor();
                executor.execute(query);
                break;
            case UPDATE:
                UpdateQueryExecutor updateQueryExecutor = new UpdateQueryExecutor();
                updateQueryExecutor.execute(query);
                break;
            case DELETE:
                DeleteQueryExecutor deleteQueryExecutor = new DeleteQueryExecutor();
                deleteQueryExecutor.execute(query);
                break;
            case INSERT:
                InsertQueryExecutor insertQueryExecutor = new InsertQueryExecutor();
                insertQueryExecutor.execute(query);
                break;
            case SELECT:
                SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor();
                selectQueryExecutor.execute(query);
                break;
            case DROP:
                DropTableQueryExecutor dropTableQueryExecutor = new DropTableQueryExecutor();
                dropTableQueryExecutor.execute(query);
                break;
            default:
                LOGGER.info("Provided query is not yet supported by this tool.");
                break;
        }

        long endTime = System.currentTimeMillis();
        LOGGER.debug("Execution completed for query - '{}' on {}", query, new Date(endTime));
        LOGGER.debug("Total execution time for query - '{}' is {}ms.", query, (endTime - startTime));
        LOGGER.info("Query executed in {}ms.", (endTime - startTime));
    }
}
