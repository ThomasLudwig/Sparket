package fr.inserm.u1078.sparket;

import htsjdk.variant.vcf.VCFHeaderLine;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.ds.ADAMContext;
import org.bdgenomics.adam.ds.variant.VariantDataset;

public class Main {
  public static void main(String[] args){

    if(args.length < 1){
      System.err.println("Missing arguments");
      System.exit(1);
    }

    String filename = args[0];
    VariantDataset variants = getVariants(filename);
    VCFHeaderLine last = variants.headerLines().last();
    int headerSize = variants.headerLines().size();
    System.out.println("There are "+headerSize+" lines in the header");
    System.out.println("The Last in is:");
    System.out.println(last.toString());
    System.out.println("toString");
    System.out.println(variants.toString());
  }

  private static VariantDataset getVariants(String filename){
    return getContext().loadVariants(filename);
  }

  public static JavaADAMContext getContext(){
    SparkSession spark = SparkSession.builder().appName("Sparket").getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    ADAMContext ac = new ADAMContext(jsc.sc());
    // then wrap that in a JavaADAMContext
    return new JavaADAMContext(ac);
  }
}
