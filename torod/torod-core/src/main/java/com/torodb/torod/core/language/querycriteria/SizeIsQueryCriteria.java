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


package com.torodb.torod.core.language.querycriteria;

import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.querycriteria.utils.QueryCriteriaVisitor;
import com.torodb.torod.core.subdocument.values.IntegerValue;
import com.torodb.torod.core.subdocument.values.Value;

/**
 *
 */
public class SizeIsQueryCriteria extends AttributeAndValueQueryCriteria {
    private static final long serialVersionUID = 1L;

    public SizeIsQueryCriteria(AttributeReference attributeReference, IntegerValue val) {
        super(attributeReference, val);
    }

    @Override
    protected int getBaseHash() {
        return 23;
    }

    @Override
    public IntegerValue getValue() {
        return (IntegerValue) super.getValue();
    }

    @Override
    public String toString() {
        return getAttributeReference() + "->size() == " + getValue();
    }

    @Override
    public <Result, Arg> Result accept(QueryCriteriaVisitor<Result, Arg> visitor, Arg arg) {
        return visitor.visit(this, arg);
    }
    
}
