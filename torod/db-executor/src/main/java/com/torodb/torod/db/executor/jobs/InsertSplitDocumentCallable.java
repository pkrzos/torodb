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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep2;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.SQLDataType;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.WriteError;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.DbException;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import com.torodb.torod.db.executor.DefaultSessionTransaction;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.TorodbMeta;

/**
 *
 */
public class InsertSplitDocumentCallable implements Callable<InsertResponse> {

    private final DefaultSessionTransaction.DbConnectionProvider connectionProvider;
    private final String collection;
    private final Collection<SplitDocument> docs;
    private final WriteFailMode mode;

    public InsertSplitDocumentCallable(
            DefaultSessionTransaction.DbConnectionProvider connectionProvider,
            String collection,
            Collection<SplitDocument> docs,
            WriteFailMode mode) {

        this.connectionProvider = connectionProvider;
        this.collection = collection;
        this.docs = docs;
        this.mode = mode;
    }

    @Override
    public InsertResponse call() throws Exception {
        switch (mode) {
            case ISOLATED:
                return isolatedInsert();
            case ORDERED:
                return orderedInsert();
            case TRANSACTIONAL:
                return transactionalInsert();
            default:
                throw new AssertionError("Study exceptions");
        }
    }

    private void insertSingleDoc(SplitDocument doc) throws ImplementationDbException, UserDbException {
        DbConnection connection = connectionProvider.getConnection();
//        connection.insertRootDocuments(collection, Collections.singleton(doc));
        
        try {
        	Configuration conf = new DefaultConfiguration()
        	.set(SQLDialect.POSTGRES);
        	DSLContext dslContext = DSL.using(conf);
        	CollectionSchema colSchema =  new TorodbMeta(dslContext).getCollectionSchema(collection);

            Field<Integer> idField = DSL.field("did", SQLDataType.INTEGER.nullable(false));
            Field<Integer> sidField = DSL.field("sid", SQLDataType.INTEGER.nullable(false));
          
           
            InsertValuesStep2<Record, Integer, Integer> insertInto = dslContext.insertInto(DSL.tableByName(colSchema.getName(), "root"), idField, sidField);

            for (SplitDocument splitDocument : docs) {
                int structureId = colSchema.getStructuresCache().getOrCreateStructure(splitDocument.getRoot(), dslContext);

                insertInto = insertInto.values(splitDocument.getDocumentId(), structureId);
            }

            insertInto.execute();
            
        } catch (DataAccessException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        for (SubDocType type : doc.getSubDocuments().rowKeySet()) {
            ImmutableCollection<SubDocument> subDocs = doc.getSubDocuments().row(type).values();

            connection.insertSubdocuments(collection, type, subDocs.iterator());
        }
    }

    private InsertResponse isolatedInsert() throws ImplementationDbException {
        int index = 0;
        List<WriteError> errors = Lists.newLinkedList();
        for (SplitDocument doc : docs) {
            try {
                insertSingleDoc(doc);

                index++;
            } catch (UserDbException ex) {
                appendError(errors, ex, index);
            }
        }

        return createResponse(docs.size(), errors);
    }

    private InsertResponse orderedInsert() throws ImplementationDbException {
        int index = 0;
        List<WriteError> errors = Lists.newLinkedList();
        try {
            for (SplitDocument doc : docs) {
                insertSingleDoc(doc);

                index++;
            }
        } catch (UserDbException ex) {
            appendError(errors, ex, index);
        }

        return createResponse(docs.size(), errors);
    }

    private InsertResponse transactionalInsert() throws ImplementationDbException {
        DbConnection connection = connectionProvider.getConnection();
        List<WriteError> errors = Lists.newLinkedList();
        
        /*
		* First we need to store the root documents
		*/
//            connection.insertRootDocuments(collection, docs);
		   Configuration conf = new DefaultConfiguration()
		   .set(SQLDialect.POSTGRES);
		   DSLContext dslContext = DSL.using(conf);
		try {
			CollectionSchema colSchema =  new TorodbMeta(dslContext).getCollectionSchema(collection);

		    Field<Integer> idField = DSL.field("did", SQLDataType.INTEGER.nullable(false));
		    Field<Integer> sidField = DSL.field("sid", SQLDataType.INTEGER.nullable(false));

		    
		    InsertValuesStep2<Record, Integer, Integer> insertInto = dslContext.insertInto(DSL.tableByName(colSchema.getName(), "root"), idField, sidField);

		    for (SplitDocument splitDocument : docs) {
		        int structureId = colSchema.getStructuresCache().getOrCreateStructure(splitDocument.getRoot(), dslContext);

		        insertInto = insertInto.values(splitDocument.getDocumentId(), structureId);
		    }

		    insertInto.execute();
		    
		} catch (DataAccessException ex) {
		    //TODO: Change exception
		    throw new RuntimeException(ex);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		* Then we have to store the subdocuments. It is more efficient to do one insert for each table, so inserts
		* are done by subdocument type.
		* To do that, we could create a map like Map<SubDocType, List<SubDocument>> and then iterate over the keys,
		* but we need to duplicate memory and the documents to insert may be very big. So we decided to do it in a
		* functional way. First we get all types and then we use an iterator that, for each type 't' and document 'd'
		* does d.getSubDocuments().row(k).values().iterator and finally merges the iterators grouped by type.
		*/
		Set<SubDocType> types = Sets.newHashSetWithExpectedSize(10 * docs.size());
		for (SplitDocument splitDocument : docs) {
		    types.addAll(splitDocument.getSubDocuments().rowKeySet());
		}
		
		/*
		* The following code that uses guava functions is the same as the following jdk8 code:
		* for (SubDocType type : types) {
		*   java.util.function.Function<SplitDocument, Stream<SubDocument>> f = (sd) -> sd.getSubDocuments().row(type).values().stream();
		*
		*   Stream<SubDocument> flatMap = docs.stream().map(f).flatMap((stream) -> stream);
		*
		*   connection.insertSubdocuments(collection, type, flatMap.iterator());
		*
		* }
		*/
		for (SubDocType type : types) {
		    Function<SplitDocument, Iterable<SubDocument>> extractor = new SubDocumentExtractorFunction(type);
		    
		    connection.insertSubdocuments(collection, type, Iterables.concat(Iterables.transform(docs, extractor)).iterator());
		}
		
		return createResponse(docs.size(), errors);
    }

    private InsertResponse createResponse(int docInserted, @Nonnull List<WriteError> errors) {
        if (errors.isEmpty()) {
            assert docInserted == docs.size();
            return new InsertResponse(true, docInserted, null);
        }
        return new InsertResponse(false, docInserted, ImmutableList.copyOf(errors));
    }

    private void appendError(@Nonnull List<WriteError> errors, DbException ex, int index) {
        errors.add(new WriteError(index, getErrorCode(ex), getErrorMessage(ex)));
    }

    private int getErrorCode(DbException ex) {
        return -1;
    }

    private String getErrorMessage(DbException ex) {
        return ex.getMessage();
    }

    /**
     * This function extracts the subdocuments of a given type contained in a given
     * {@linkplain SubDocument subdocuments}.
     */
    private static class SubDocumentExtractorFunction implements Function<SplitDocument, Iterable<SubDocument>> {

        private final SubDocType type;

        public SubDocumentExtractorFunction(SubDocType type) {
            this.type = type;
        }

        @Override
        public Iterable<SubDocument> apply(SplitDocument input) {
            if (input == null) {
                throw new IllegalArgumentException();
            }
            return input.getSubDocuments().row(type).values();
        }
    }
}
