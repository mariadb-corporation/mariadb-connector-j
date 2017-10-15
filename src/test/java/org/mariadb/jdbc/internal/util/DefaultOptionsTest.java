package org.mariadb.jdbc.internal.util;

import org.junit.Test;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.Assert.*;

public class DefaultOptionsTest {

    @Test
    public void parseDefault() throws Exception {
        //check that default option object correspond to default
        Options option = new Options();
        DefaultOptions.optionCoherenceValidation(option);
        for (HaMode haMode : HaMode.values()) {
            Options defaultOption = DefaultOptions.parse(haMode, "", new Properties(), null);
            DefaultOptions.optionCoherenceValidation(defaultOption);
            for (DefaultOptions o : DefaultOptions.values()) {
                Field field = Options.class.getField(o.getOptionName());
                assertEquals("field :" + field.getName(), field.get(o.defaultValues(HaMode.NONE)), field.get(option));
                assertEquals("field :" + field.getName(), field.get(o.defaultValues(haMode)), field.get(defaultOption));
            }
        }
    }

    /**
     * Ensure that default value of new Options() correspond to DefaultOption Enumeration.
     * @throws Exception if any value differ.
     */
    @Test
    public void parseOption() throws Exception {
        Options option = new Options();
        String param = generateParam();
        for (HaMode haMode : HaMode.values()) {
            Options resultOptions = DefaultOptions.parse(haMode, param, new Properties(), null);
            for (Field field : Options.class.getFields()) {
                if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    if ("java.lang.String".equals(field.getType().getName())) {
                        assertEquals("field " + field.getName() + " value error for param" + param,
                                field.get(resultOptions), field.getName() + "1");
                    } else if ("int".equals(field.getType().getName())) {
                        assertEquals("field " + field.getName() + " value error for param" + param,
                                field.getInt(resultOptions), 9999);
                    } else if ("java.lang.Integer".equals(field.getType().getName())) {
                        assertEquals("field " + field.getName() + " value error for param" + param,
                                ((Integer) field.get(resultOptions)).intValue(), 9999);
                    } else if ("java.lang.Long".equals(field.getType().getName())) {
                        assertEquals("field " + field.getName() + " value error for param" + param,
                                ((Long) field.get(resultOptions)).intValue(), 9999);
                    } else if ("java.lang.Boolean".equals(field.getType().getName())) {
                        Boolean bool = (Boolean) field.get(option);
                        if (bool == null) {
                            assertTrue("field " + field.getName() + " value error for param" + param,
                                    (Boolean) field.get(resultOptions));
                        } else {
                            assertEquals("field " + field.getName() + " value error for param" + param,
                                    (Boolean) field.get(resultOptions), !bool);
                        }
                    } else if ("boolean".equals(field.getType().getName())) {
                        System.out.println(field.getName() + ": " + field.getBoolean(resultOptions) + " " + field.get(resultOptions));
                        assertEquals("field " + field.getName() + " value error for param" + param,
                                field.getBoolean(resultOptions), !field.getBoolean(option));
                    } else {
                        fail("type not used normally ! " + field.getType().getName());
                    }
                }
            }
        }
    }

    private String generateParam() throws IllegalAccessException {
        Options option = new Options();
        //check option url settings
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (DefaultOptions defaultOption : DefaultOptions.values()) {
            for (Field field : Options.class.getFields()) {
                if (defaultOption.getOptionName().equals(field.getName())) { //for having same order
                    if (first) {
                        first = false;
                    } else {
                        sb.append("&");
                    }
                    sb.append(field.getName()).append("=");
                    if ("java.lang.String".equals(field.getType().getName())) {
                        sb.append(field.getName()).append("1");
                    } else if ("java.lang.Integer".equals(field.getType().getName())
                            || "java.lang.Long".equals(field.getType().getName())
                            || "int".equals(field.getType().getName())) {
                        sb.append("9999");
                    } else if ("java.lang.Boolean".equals(field.getType().getName())) {
                        Boolean bool = (Boolean) field.get(option);
                        if (bool == null) {
                            sb.append("true");
                        } else {
                            sb.append((!((Boolean) field.get(option))));
                        }
                    } else if ("boolean".equals(field.getType().getName())) {
                        sb.append(!(Boolean) (field.get(option)));
                    } else {
                        fail("type not used normally ! : " + field.getType().getName());
                    }
                }
            }
        }
        return sb.toString();
    }

    @Test
    public void buildTest() throws Exception {
        String param = generateParam();
        for (HaMode haMode : HaMode.values()) {
            Options resultOptions = DefaultOptions.parse(haMode, param, new Properties(), null);
            StringBuilder sb = new StringBuilder();
            DefaultOptions.propertyString(resultOptions, haMode, sb);

            assertEquals("?" + param, sb.toString());
        }
    }

}