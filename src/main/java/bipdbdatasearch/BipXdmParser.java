package bipdbdatasearch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import auxiliary.StringOperations;
import auxiliary.system.SystemFileManager;

public class BipXdmParser {

    public static ArrayList<ArrayList<String>> processXdmFilesDir(String dir) throws IOException {
        ArrayList<ArrayList<String>> array = new ArrayList<ArrayList<String>>(); // used to return parsed logs array
        // *** Create header row
        array.add(new ArrayList<String>()); // add first row at index 0
        array.get(0).add("Filename");
        array.get(0).add("Line");
        array.get(0).add("BIP_DataSource");
        array.get(0).add("DB_Schema_SQL");
        // *** Start scanning dir and sorting files
        // *** Get only '*.xdm' files.
        ArrayList<String> files = SystemFileManager.listFilesAsArrayListString(dir, "*.xdm"); // used to manage filenames in dir
        // Build full path of file and process each of them
        System.out.println("Processing file:");
        for (String str : files) {
            String paths = dir + str;
            //System.out.print("\033[2K\r"); // Erase line content
            System.out.print(str);
            // *** Call parseXdmFile() method that parsers each file and add its result (array as well) to return array
            array.addAll(parseXdmFile(paths));
        }
        System.out.println();
        System.out.println("All files in " +dir +" have been processed and parsed");
        return array;
    }


    public static ArrayList<ArrayList<String>> parseXdmFile(String filepath) throws IOException {
        ArrayList<ArrayList<String>> array = new ArrayList<ArrayList<String>>();
        File file = new File(filepath);
        Scanner filetoscan = new Scanner(file);
        int row = 0;
        String[] schema = {"FROM", "from", "From", "JOIN", "join", "Join"}; // search SQL direct schema calls
        String[] bip_ds = {"defaultDataSourceRef=", "dataSourceRef="}; // search configured bip datasources
        String datasourcevalue = "";
        int filelinecounter=1; //counter liczący procesowane linie
        int ds_occur_count=0; //counter liczący występowania datasourców w pliku. Potrzebne do if-a identyfikującego kolejne datasourcy
        int schema_occur_count=0; //counter liczący występowania schemy w danej linii. Potrzebne do if-a identyfikującego kolejne schemy per linia
        while (filetoscan.hasNext()) {
            String line = filetoscan.nextLine();
            filelinecounter += 1;
            // *** BIP datasource search
            // if line contains datasource than put in firs column
            if (StringOperations.checkIfContainWords(line, bip_ds)) {
                // *** identify exact name datasource that was found
                String bip_ds_found = StringOperations.returnIfWordIsFound(line, bip_ds);
                // *** find the beggining position of found datasource
                int bip_ds_pos = line.indexOf(bip_ds_found);
                // *** find the end position of line of found datasource
                int bip_ds_pos_end = line.length();
                // *** substring datasource name (bip_ds) located after position of datasource
                if (bip_ds_found.equals("defaultDataSourceRef=")) {
                    datasourcevalue = line.substring(bip_ds_pos + 22, bip_ds_pos_end);
                }
                else { // *** (bip_ds_found.equals("dataSourceRef=")
                    datasourcevalue = line.substring(bip_ds_pos + 15, bip_ds_pos_end);
                }

                System.out.println();
                System.out.println("BIP datasource found: " +datasourcevalue +" at position: " +filelinecounter);

                if (ds_occur_count == 0) {
                    // Add it to array in first column
                    // **** Add an new empty element (row) to each array dimension. If not .get(index) will always throw 'out of bounds' error.
                    array.add(new ArrayList<String>()); // *** Proper pick of row is controlled by row counter
                    array.get(row).add(filepath);
                    array.get(row).add(String.valueOf(filelinecounter));
                    array.get(row).add(datasourcevalue);
                    ds_occur_count += 1; // increment data source occurance counter
                }
                // if second datasource found without any SQL between, add it at next position
                else {
                    // First row with columns 1-3 filled (filepath,filelinecounter,datasourcevalue) already exists and
                    // expects 4th column (schema) to be filled, but we need to skip to next row in array
                    // *** Add an new empty element (row) to each array dimension.
                    // *** If not .get(index) will always throw 'out of bounds' error.
                    array.add(new ArrayList<String>()); // *** Proper pick of row is controlled by row counter
                    row += 1; // add data to next row instead of current one (usually 0) and continue substring schemas
                    array.get(row).add(filepath);
                    array.get(row).add(String.valueOf(filelinecounter));
                    array.get(row).add(datasourcevalue);
                    ds_occur_count += 1; // increment data source occurance counter
                }
            }

            // *** SQL schema (FROM, JOIN) calls search
            // *** if line contains FROM or JOIN strings after which schema is expected
            else if (StringOperations.checkIfContainWords(line,schema)) {
                // *** identify exact schema that was found
                String schema_found = StringOperations.returnIfWordIsFound(line, schema);
                // *** find the beggining position of found schema
                int schema_pos = line.indexOf(schema_found);
                // *** find the end position of found schema
                int schema_pos_end = line.length();
                // *** substring data located after schema_pos (remove 4 chars of FROM or JOIN)
                String schema_value = line.substring(schema_pos + 4, schema_pos_end);

                // *** Remove leading and trailing spaces from the string
                // *** trim() uses codepoint(ASCII) and removes chars having ASCII value less than or equal to ‘U+0020’ or '32' (Since Java 1)
                // *** strip() uses Unicode charset and removes spaces having different unicode. (Since Java 11)
                schema_value = schema_value.strip();

                System.out.println();
                System.out.println("Schema value found: " + schema_value + " at position: " + filelinecounter);

                // jezeli po FROM i JOIN jest pusto lub sam nawias to znaczy ze schema jest w nastepnej linii
                // lub jest selectem (nie obslugiwane)
                if (schema_value.equals("") || schema_value.equals("(")) {
                    line = filetoscan.nextLine();
                    filelinecounter = filelinecounter + 1;
                    // i ta nastepna linia staje sie naszym schema_value zamiast wczesniejszego substringa (de facto pustego)
                    schema_value = line;
                }
                // jezeli jednak w pierwszym substringu lub kolejnej linii (wynikajacej z w/w logiki)
                // jest kolejny FROM lub JOIN to wrzuc orginalny substring lub linie na pozycje 4 po przycieciu go
                // i przeparsuj go ponownie (glebokosc bedzie tylko do 2 wystapień per linie,
                // czyli jezeli wystepuje 3,4,5 FROM lub JOIN to juz ich nie parsujemy
                if (StringOperations.checkIfContainWords(schema_value, schema)) {
                    // przepisz schema_value fo new_schema_value
                    String new_schema_value = schema_value;
                    // znajdz pozycje
                    schema_found = StringOperations.returnIfWordIsFound(schema_value, schema);
                    schema_pos = schema_value.indexOf(schema_found);
                    schema_pos_end = schema_value.length();
                    // przytnij orginał do miejsca kolejnego wystapienia FROM lub JOIN i wrzuć do 4 kolumny
                    schema_value = schema_value.substring(0, schema_pos);

                    // *** Remove leading and trailing spaces from the string
                    // *** trim() uses codepoint(ASCII) and removes chars having ASCII value less than or equal to ‘U+0020’ or '32' (Since Java 1)
                    // *** strip() uses Unicode charset and removes spaces having different unicode. (Since Java 11)
                    schema_value = schema_value.strip();

                    if (schema_occur_count == 0) { // jezeli 1 wystapienie to doklej na 4 pozycji dla biezacego rowka
                        array.get(row).add(schema_value);
                        schema_occur_count += 1;
                    }
                    else { // jezeli kolejne wystapienie to wklej do kolejnego wiersza kopiując 3 pierwsze kolumny i doklej na 4 pozycji wartość
                        array.add(new ArrayList<String>()); // *** Proper pick of row is controlled by row counter
                        row += 1;
                        array.get(row).add(filepath);
                        array.get(row).add(String.valueOf(filelinecounter));
                        array.get(row).add(datasourcevalue);
                        array.get(row).add(schema_value);
                        schema_occur_count += 1;
                    }
                    System.out.println();
                    System.out.println("Another schema value found in the same line: " + new_schema_value + " at position: " + filelinecounter);
                    // przytnij string z nowym wystąpieniem od miejsca kolejnego wystapienia FROM lub JOIN
                    new_schema_value = new_schema_value.substring(schema_pos + 4, schema_pos_end);

                    // *** Remove leading and trailing spaces from the string
                    // *** trim() uses codepoint(ASCII) and removes chars having ASCII value less than or equal to ‘U+0020’ or '32' (Since Java 1)
                    // *** strip() uses Unicode charset and removes spaces having different unicode. (Since Java 11)
                    new_schema_value = new_schema_value.strip();

                    System.out.println();
                    System.out.println("Schema value substring: " + new_schema_value + " at position: " + filelinecounter);
                    // przeskocz do nowego rowka, skopiuj 3 pierwsze kolumny i umiesc substring new_schema_value na 4 pozycji
                    // *** Add an new empty element (row) to each array dimension. If not .get(index) will always throw 'out of bounds' error.
                    array.add(new ArrayList<String>()); // *** Proper pick of row is controlled by row counter
                    row += 1;
                    array.get(row).add(filepath);
                    array.get(row).add(String.valueOf(filelinecounter));
                    array.get(row).add(datasourcevalue);
                    array.get(row).add(new_schema_value);
                }
                else { // jezeli jednak nie ma kolejnego wystąpienia FROM/JOIN w tej samej linii to po prostu wklej na 4 pozycji
                    if (schema_occur_count == 0) { // jezeli 1 wystapienie to doklej na 4 pozycji dla biezacego rowka
                        array.get(row).add(schema_value);
                        schema_occur_count += 1;
                    }
                    else { // jezeli kolejne wystapienie to wklej do kolejnego wiersza kopiując 3 pierwsze kolumny i doklej na 4 pozycji wartość
                        array.add(new ArrayList<String>()); // *** Proper pick of row is controlled by row counter
                        row += 1;
                        array.get(row).add(filepath);
                        array.get(row).add(String.valueOf(filelinecounter));
                        array.get(row).add(datasourcevalue);
                        array.get(row).add(schema_value);
                        schema_occur_count += 1;
                    }
                }
            }
            else{
                // do nothing for other lines
            }
            filetoscan.close();
        }
        return array;
    }
}

