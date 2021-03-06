/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.db.executor;

import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.db.executor.jobs.*;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

/**
 *
 */
public class DefaultSessionTransaction implements SessionTransaction {

    private final DbWrapper dbWrapper;
    private final DefaultSessionExecutor executor;
    private final DbConnectionProvider connectionProvider;
    private boolean closed;

    DefaultSessionTransaction(DefaultSessionExecutor executor, DbWrapper dbWrapper) {
        this.executor = executor;
        this.dbWrapper = dbWrapper;
        this.connectionProvider = new DbConnectionProvider(dbWrapper);
        closed = false;
    }

    @Override
    public Future<?> rollback() {
        return executor.submit(new RollbackCallable(connectionProvider));
    }

    @Override
    public Future<?> commit() {
        return executor.submit(new CommitCallable(connectionProvider));
    }

    @Override
    public void close() {
        closed = true;
        executor.submit(new CloseConnectionCallable(connectionProvider));
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Future<InsertResponse> insertSplitDocuments(String collection, Collection<SplitDocument> documents, WriteFailMode mode)
            throws ToroTaskExecutionException {
        return executor.submit(new InsertSplitDocumentCallable(connectionProvider, collection, documents, mode));
    }

    @Override
    public Future<Void> query(String collection, CursorId cursorId, QueryCriteria filter, Projection projection) {
        return executor.submit(new QueryCallable(connectionProvider, collection, cursorId, filter, projection));
    }

    @Override
    public Future<List<? extends SplitDocument>> readCursor(CursorId cursorId, int limit)
            throws ToroTaskExecutionException {
        return executor.submit(new ReadCursorCallable(connectionProvider, cursorId, limit));
    }

    @Override
    public Future<List<? extends SplitDocument>> readAllCursor(CursorId cursorId)
            throws ToroTaskExecutionException {
        return executor.submit(new ReadAllCursorCallable(connectionProvider, cursorId));
    }

    @Override
    public Future<DeleteResponse> delete(String collection, List<? extends DeleteOperation> deletes, WriteFailMode mode) {
        return executor.submit(new DeleteCallable(connectionProvider, collection, deletes, mode));
    }

    @Override
    public Future<Integer> countRemainingDocs(CursorId cursorId) {
        return executor.submit(new CountRemainingDocs(connectionProvider, cursorId));
    }

    @Override
    public Future<?> closeCursor(CursorId cursorId) throws
            ToroTaskExecutionException {
        return executor.submit(new CloseCursorCallable(connectionProvider, cursorId));
    }

    public static class DbConnectionProvider {

        private final DbWrapper dbWrapper;
        private DbConnection connection;

        public DbConnectionProvider(DbWrapper dbWrapper) {
            this.dbWrapper = dbWrapper;
        }

        public DbConnection getConnection() {
            if (connection == null) {
                try {
                    connection = dbWrapper.consumeSessionDbConnection();
                }
                catch (ImplementationDbException ex) {
                    throw new ToroImplementationException(ex);
                }
                assert connection != null;
            }
            return connection;
        }
    }

}
