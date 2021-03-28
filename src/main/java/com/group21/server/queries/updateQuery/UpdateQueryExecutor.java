package com.group21.server.queries.updateQuery;

public class UpdateQueryExecutor {
    private UpdateQueryParser updateQueryParser;

    public UpdateQueryExecutor(){
        this.updateQueryParser = new UpdateQueryParser();
    }

    public void execute(String query){
        updateQueryParser.isValid(query);
    }
}
