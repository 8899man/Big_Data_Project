package edu.itu.hadoop;

public class StartPoint {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Please, use 'gen', 'sort' or 'validate' as first argument");
            return;
        }
        String task = args[0].toLowerCase();
        if (task.equals("gen")) {
            DataGenerator.main(args);
        } else if (task.equals("sort")) {
            DataSorter.main(args);
        } else if (task.equals("validate")) {
            DataValidator.main(args);
        } else {
            System.err.println("Please, use 'gen', 'sort' or 'validate' as first argument");
        }
    }
}
