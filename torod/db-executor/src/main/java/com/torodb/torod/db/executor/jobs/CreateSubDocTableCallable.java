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

package com.torodb.torod.db.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.executor.SystemExecutor;
import com.torodb.torod.core.subdocument.SubDocType;
import javax.annotation.Nullable;

/**
 *
 */
public class CreateSubDocTableCallable extends SystemDbCallable<Void> {

    private final String collection;
    private final SubDocType type;
    private final @Nullable SystemExecutor.CreateSubDocTypeTableCallback callback;

    public CreateSubDocTableCallable(
            DbWrapper dbWrapperPool,
            String collection,
            SubDocType type,
            SystemExecutor.CreateSubDocTypeTableCallback callback) {

        super(dbWrapperPool);
        this.collection = collection;
        this.type = type;
        this.callback = callback;
    }

    @Override
    Void call(DbConnection db) throws ImplementationDbException {
        db.createSubDocTypeTable(collection, type);
        return null;
    }

    @Override
    void doCallback() {
        if (callback != null) {
            callback.createSubDocTypeTable(collection, type);
        }
    }
}
