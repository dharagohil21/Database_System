package com.group21.server.queries.updateQuery;

public class UpdateQueryExecutor {
    private UpdateParser updateQueryParser;

    public UpdateQueryExecutor(){
        this.updateQueryParser = new UpdateParser();
    }

    public void execute(String query){
        updateQueryParser.isValid(query);
    }
}
