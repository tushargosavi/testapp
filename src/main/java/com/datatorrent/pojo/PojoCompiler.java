package com.datatorrent.pojo;

import org.omg.PortableServer.POAHelper;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Use a simple pojo to create a memory optimized version
 * of it with information.
 */
public class PojoCompiler
{
  private final Class klass;
  List<Field> fieldList;
  List<Method> getterList;

  Map<String, String> fieldExpressions;


  public PojoCompiler(Class klass)
  {
    this.klass = klass;
  }

  static class ObjDescriptor
  {
    int size;
  }

  public ObjDescriptor processClass() {
    int size = 0;
    Field[] fields = klass.getDeclaredFields();
    for (Field f : fields) {
      if (typeToSizeMap.containsKey(f.getType())) {
        size += typeToSizeMap.get(f.getType());
      }
    }
    ObjDescriptor desc = new ObjDescriptor();
    desc.size = size;

    return desc;
  }

  public static void main(String[] args)
  {

  }

  static Map<Class, Integer> typeToSizeMap;
  static {
    typeToSizeMap.put(Integer.class, 4);
    typeToSizeMap.put(Integer.TYPE, 4);
    typeToSizeMap.put(Float.class, 4);
    typeToSizeMap.put(Float.TYPE, 4);
    typeToSizeMap.put(Long.class, 8);
    typeToSizeMap.put(Long.TYPE, 8);
    typeToSizeMap.put(Double.class, 8);
    typeToSizeMap.put(Double.TYPE, 8);
    typeToSizeMap.put(Short.class, 2);
    typeToSizeMap.put(Short.TYPE, 2);
    typeToSizeMap.put(Byte.TYPE, 1);
    typeToSizeMap.put(Byte.class, 1);
    typeToSizeMap.put(Boolean.class, 1);
    typeToSizeMap.put(Boolean.TYPE, 1);
  }

  public static int typeToSize(Class klass)
  {
    if (typeToSizeMap.containsKey(klass))
      return typeToSizeMap.get(klass);

    throw new RuntimeException("Class can not be added to jar");
  }

}
