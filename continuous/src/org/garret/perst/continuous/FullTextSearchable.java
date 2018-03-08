package org.garret.perst.continuous;

import java.lang.annotation.*;

/**
  * Annotation for full text searchable field
  */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FullTextSearchable { 
}