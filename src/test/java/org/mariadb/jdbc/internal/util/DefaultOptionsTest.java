package org.mariadb.jdbc.internal.util;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.jdbc.Driver;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
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

    @Test
    public void getDefaultPropertyInfo() throws Exception {
        //check that default option object correspond to default
        Driver driver = new Driver();
        DriverPropertyInfo[] driverPropertyInfos = driver.getPropertyInfo("jdbc:mariadb://localhost:3306/testj?user=hi",
                new Properties());
        assertTrue(driverPropertyInfos.length > 30);
        Assert.assertEquals(driverPropertyInfos[0].name, "user");
        Assert.assertEquals(driverPropertyInfos[0].value, "hi");
        Assert.assertEquals(driverPropertyInfos[0].description, "Database user name");
    }


    @Test
    public void parseDefaultDriverManagerTimeout() throws Exception {
        DriverManager.setLoginTimeout(2);
        UrlParser parser = UrlParser.parse("jdbc:mariadb://localhost/");
        assertEquals(parser.getOptions().connectTimeout, 2_000);

        DriverManager.setLoginTimeout(0);

        parser = UrlParser.parse("jdbc:mariadb://localhost/");
        assertEquals(parser.getOptions().connectTimeout, 30_000);
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
                    switch (field.getType().getName()) {
                        case "java.lang.String":
                            assertEquals("field " + field.getName() + " value error for param" + param,
                                    field.get(resultOptions), field.getName() + "1");
                            break;
                        case "int":
                            assertEquals("field " + field.getName() + " value error for param" + param,
                                    field.getInt(resultOptions), 9999);
                            break;
                        case "java.lang.Integer":
                            assertEquals("field " + field.getName() + " value error for param" + param,
                                    ((Integer) field.get(resultOptions)).intValue(), 9999);
                            break;
                        case "java.lang.Long":
                            assertEquals("field " + field.getName() + " value error for param" + param,
                                    ((Long) field.get(resultOptions)).intValue(), 9999);
                            break;
                        case "java.lang.Boolean":
                            Boolean bool = (Boolean) field.get(option);
                            if (bool == null) {
                                assertTrue("field " + field.getName() + " value error for param" + param,
                                        (Boolean) field.get(resultOptions));
                            } else {
                                assertEquals("field " + field.getName() + " value error for param" + param,
                                        (Boolean) field.get(resultOptions), !bool);
                            }
                            break;
                        case "boolean":
                            assertEquals("field " + field.getName() + " value error for param" + param,
                                    field.getBoolean(resultOptions), !field.getBoolean(option));
                            break;
                        default:
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
                    switch (field.getType().getName()) {
                        case "java.lang.String":
                            sb.append(field.getName()).append("1");
                            break;
                        case "int":
                        case "java.lang.Integer":
                        case "java.lang.Long":
                            sb.append("9999");
                            break;
                        case "java.lang.Boolean":
                            Boolean bool = (Boolean) field.get(option);
                            if (bool == null) {
                                sb.append("true");
                            } else {
                                sb.append((!((Boolean) field.get(option))));
                            }
                            break;
                        case "boolean":
                            sb.append(!(boolean) (field.get(option)));
                            break;
                        default:
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