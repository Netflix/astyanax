/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.thrift;

public enum ThriftTypes {
    STOP   ,
    VOID   ,
    BOOL   ,
    BYTE   ,
    DOUBLE ,
    IGNORE1,
    I16    ,
    IGNORE2,
    I32    ,
    IGNORE3,
    I64    ,
    STRING ,
    STRUCT ,
    MAP    ,
    SET    ,
    LIST   ,
    ENUM   
}
