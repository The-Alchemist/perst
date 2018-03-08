package org.garret.perst.continuous;

import java.lang.annotation.*;

/**
  * Annotation for not versioned class
  */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NotVersioned { 
}