package bipdbdatasearch;

import java.io.IOException;
import java.util.ArrayList;
import auxiliary.filetype.xlsx;

public class Main {

    public static void main(String[] args) throws IOException {
        System.out.println("<--- START --->");
        long startTime = System.nanoTime();
        String root_folder = "C:\\BIP_REPORTS\\";
        String input_subfolder = "xdm";
        String output_folder = "xlsx";
        String output_xlsx_filename = "result.xlsx";

        String dir = root_folder+"\\"+input_subfolder+"\\";
        // utwórz plik excela na wyniki
        xlsx excel = new xlsx();
        excel.createEmptyFile(root_folder+"\\"+output_folder+"\\"+output_xlsx_filename, input_subfolder);
        // utwóz obiekt macierzy
        ArrayList<ArrayList<String>> array2d = new ArrayList<ArrayList<String>>();
        // when initializing create Excel header line
        array2d = BipXdmParser.processXdmFilesDir(dir);
        excel.appendWorkbookOnFirstSheet(root_folder+"\\"+output_folder+"\\"+output_xlsx_filename, array2d);
        long stopTime = System.nanoTime();
        // elapsed time in seconds
        double elapsedTime = (double) (stopTime - startTime) / 1_000_000_000;
        System.out.println("Code Execution Time: " +elapsedTime +" seconds");
        System.out.println("<--- END --->");
    }

}



