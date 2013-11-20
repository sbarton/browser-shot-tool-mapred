/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.internetmemory.browsershots;

import Taverna.ScapeTest;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.imageio.ImageIO;
import net.internetmemory.utils.HdfsDir;
import net.internetmemory.utils.HdfsFile;
import net.internetmemory.utils.TrackerNumber;
import net.internetmemory.utils.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
///import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.Platform;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.remote.Augmenter;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.ErrorHandler;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.slf4j.LoggerFactory;

/**
 * Mapreduce job to parallel similarity computation of a browsershot comparison 
 * using marcalyzer
 * @author sbarton
 */
public class PageRenderCompareMapRed {
    
    public static TrackerNumber tracker = new TrackerNumber();
    
    private static final long MAX_WAIT_S = 45;
    
    private static final String seleniumUrl = "http://im1a10.internetmemory.org:5550/wd/hub";
    
    private static final String BROWSERS_REF = "im.browsers";
    private static final String DEFINITION_REF = "im.definition";
    
    private int numLines;
    
    Configuration conf = new Configuration();
    
    
    public static class SCAPEMapper extends Mapper<LongWritable, Text, Text, ResultWritable> {
        
        //public List<WebDriver> drivers = new ArrayList();
        public List<BrowserRep> drivers = new ArrayList();
        
        private boolean marcalyzerInitialize;
        private ScapeTest sc;
        private String scFile;
        
        public int counter = 0;
                        
        ResultWritable res = new ResultWritable();
        
        private ExecutorService executor;
        
        private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SCAPEMapper.class);
        private int timeout = 50;
        
        /**
         * Class encapsulating the browser representation for the job
         */
        public class BrowserRep {
            WebDriver driver;
            DesiredCapabilities capabilities;
            String desc;

            public BrowserRep(WebDriver driver, DesiredCapabilities capabilities, String desc) {
                this.driver = driver;
                this.capabilities = capabilities;
                this.desc = desc;
            }                        
        }
                
        /**
         * Initialize the webdriver given the capability object
         * @param capability
         * @return 
         */
        public WebDriver initWebDriver(DesiredCapabilities capability) {
            System.out.println("this is the new version");
            tracker.startTrackerMilis("getDriver");        

            WebDriver driver = null;
            boolean done = false;
            int attemptNo = 0;
            while (!done) {
                attemptNo++;
                try {
                    System.out.println("Attempt = "+attemptNo);
                    //driver = new RemoteWebDriver(new URL("http://im1a10:8090/wd/hub"), capability);
                    driver = new RemoteWebDriver(new URL(seleniumUrl), capability);
                    done = true;
                } catch (MalformedURLException e) {
                        throw new RuntimeException("Invalid Selenium driver URL", e);
                } catch (IOException e) {
                    System.out.println("Attempt failed sleeping for 10s.");
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(PageRenderCompareMapRed.class.getName()).log(Level.SEVERE, null, ex);
                    }                
                } catch (WebDriverException e) {
                    System.out.println("Attempt failed sleeping for 10s.");
                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(PageRenderCompareMapRed.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            
            
            
            
            driver = new Augmenter().augment(driver);
            driver.manage().timeouts().pageLoadTimeout(MAX_WAIT_S, TimeUnit.SECONDS);
            driver.manage().timeouts().implicitlyWait(MAX_WAIT_S, TimeUnit.SECONDS);
            tracker.endTrackerMilis("getDriver");
            return driver;
        }
        
        /**
         * Save screenshot to disk
         * @param i
         * @param sShot
         * @param browser M
         */
        public void saveScreenShot(int i, byte[] sShot, String browser) {
            
            if (sShot == null) return;
            
            String path = "/tmp/sss/";
            File f = new File(path);
            if ( !f.exists()) {
                f.mkdir();
            }
            
            String sssName = "scnsht-"+browser+"-"+i+".png";
            try {
                FileUtils.writeByteArrayToFile(new File(f+"/"+sssName), sShot);
                
            } catch (IOException ex) {
                Logger.getLogger(PageRenderCompareMapRed.class.getName()).log(Level.SEVERE, null, ex);
            }            
        }
        
        
        private ExecutorService createExecutorService() {
            return Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "IM ProcessingPool Thread");
				t.setDaemon(true);
				return t;
			}
		});
        }
        
        /**
        * Get the executor
        */
       protected ExecutorService getExecutor() {
           return executor;
       }
        
       protected int getTimeout() {
           return timeout;
       }
       
       /**
        * Reset the executor (after a timeout)
        */
       protected void resetExecutor()
       {
           executor = createExecutorService();
       }
        
       /**
        * Add a value to the hadoop counter
        * @param name
        * @param value 
        */
       protected void addToHadoopCounter(String name, long value) {
		String className = getClass().getName();
		// Shorten the common prefix
		String prefix = "net.internetmemory.extraction";
		if (className.startsWith(prefix)) {
			className = "n.i.e" + className.substring(prefix.length());
		}

		if (tracker != null) {
			tracker.addSingleValue(className + " - "
					+ name, value);
		}
	}
       
       
        /**
         * Get the browser shot using given broser type and a URL. Use separate
         * thread to get the browser and timeout in case it gets stuck.
         * @param br
         * @param url
         * @return 
         */
        public byte[] getScreenShotWithTimeout(final BrowserRep br, final String url) {
            Callable<byte[]> extractCallable = new Callable<byte[]>() {
			@Override
			public byte[] call() throws Exception {
				return getScreenShot(br, url);
			}
		};
            
            Future<byte[]> future = getExecutor().submit(extractCallable);
            byte[] extractResult = null;
            try {
			extractResult = future.get(getTimeout(), TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			addToHadoopCounter("Interrupted", 1);
			LOG.error(
					"Interrupted while executing: " + url,
					ex);
		} catch (ExecutionException ex) {
			// Unwrap this exception with get cause which will give a more
			// meaningful error
			LOG.error("Error during execution: " + url,
					ex.getCause());			
			addToHadoopCounter("Extraction Exception", 1);
		} catch (TimeoutException ex) {
			addToHadoopCounter("Timeout", 1);
			future.cancel(true);
			
			// Try to kill the current execution context and shut it down
			getExecutor().shutdown();
			try {
				if (!getExecutor().awaitTermination(getTimeout(),
						TimeUnit.SECONDS)) {
					addToHadoopCounter("Timeout Zombie", 1);
				}
			} catch (InterruptedException e) {
			} finally {
				resetExecutor();
			}

			LOG.error("Timeout during execution: " + url,
					ex);
		}

		return extractResult;            
        }
        
        
        /**
         * Get the screenshot for a given browser and a given url
         * @param br
         * @param url
         * @return byte image representation (png)
         */
        public byte[] getScreenShot(BrowserRep br, String url) {
            File screenshot = null;
            System.out.println("getting image using driver: "+br.driver);
            byte[] screenshotBytes = null;
            try {
                tracker.startTrackerMilis("getPage-"+br.desc);
                br.driver.get(url);
                String title = br.driver.getTitle();

                System.out.println("title: "+title);
                tracker.endTrackerMilis("getPage-"+br.desc);

                tracker.startTrackerMilis("takeSnapshot-"+br.desc);
                    //screenshot = ((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
                screenshotBytes = ((TakesScreenshot)br.driver).getScreenshotAs(OutputType.BYTES);
                tracker.endTrackerMilis("takeSnapshot-"+br.desc);

                //if (screenshot != null ) {
                    tracker.addSingleValue("shotSize", screenshot != null?screenshot.length():screenshotBytes.length);
                //}                    
            //} catch(UnreachableBrowserException e) {
                } catch(WebDriverException e) {
                    System.out.println("ERROR: Could not load " + url);
                    System.out.println("Trying to reinitialize browser");
                    e.printStackTrace();
                    try {
                        br.driver.close();
                    }
                    catch (WebDriverException ex) {
                        System.out.println("ERROR: cannot close browser ");
                    }
                    
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                    } catch (InterruptedException e2) {
                    }

                br.driver = initWebDriver(br.capabilities);
                return null;
                
            } catch (Throwable e) {
                System.out.println("ERROR: Could not load " + url);
                System.out.println(e);
                return null;
            }

            //;
            //byte[] screenshotAsBytes = ((TakesScreenshot)driver).getScreenshotAs(OutputType.BYTES);
            //tracker.addSingleValue("shotSize", screenshotAsBytes.length);
            tracker.endTrackerMilis("takeSnapshot");
            return screenshotBytes;
        }   
        
                
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {     
            super.setup(context);
            
            executor = createExecutorService();
            
            String[] browsers = context.getConfiguration().getStrings(BROWSERS_REF);
            
            for (String browser : browsers) {
                tracker.startTrackerMilis("browserInit");
                System.out.println("Setting up browser: "+browser);
                DesiredCapabilities capability = null;
                if  (browser.equals("firefox")) {
                    capability = DesiredCapabilities.firefox();
                }
                else if (browser.equals("opera")) {
                    capability = DesiredCapabilities.opera();
                }
                else if (browser.equals("chrome")) {
                    capability = DesiredCapabilities.chrome();
                }else {
                    throw new RuntimeException("Browser "+browser+ " not recognized.");
                }
                
                capability.setPlatform(Platform.LINUX);                
                WebDriver driver = initWebDriver(capability);
                
                BrowserRep br = new BrowserRep(driver, capability, browser);
                
                drivers.add(br);
                tracker.endTrackerMilis("browserInit");
            }
            
            
            scFile = context.getConfiguration().get(DEFINITION_REF, "");
            
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {     
            tracker.outProgressAll(false);
            
            for (BrowserRep br : drivers) {
                br.driver.close();
            }
        }
        
        /**
         * Initialize marcalyzer tool
         */
        public void initMarcalizer() {
            marcalyzerInitialize = true;
            sc= new ScapeTest();

            if (scFile.isEmpty()) {
                //scFile = "/Users/barton/z-space/implementation/MarcAlizer/exemple/ex2.xml";
                scFile = "/0/stanislav/MarcAlizer/exemple/ex2.xml";
            }

            sc.init(new File(scFile));
            System.out.println("marcalizer initialized.");
        }
        
        /**
         * Compare two images using marcalyzer 
         * @param im1
         * @param im2
         * @return dissimilarity score
         */
        public double marcalizerCompare(BufferedImage im1, BufferedImage im2) {
            if (marcalyzerInitialize == false) {
                initMarcalizer();
            }
            double score = sc.run(im1, im2);
            return score;
        }
        
        
        
        public double getScore(byte[] image1, byte[] image2) {
            tracker.startTrackerMilis("pagelyzer");
            BufferedImage im1 = null;
            BufferedImage im2 = null;
            try {
                im1 = ImageIO.read(new ByteArrayInputStream(image1));
                im2 = ImageIO.read(new ByteArrayInputStream(image2));

                System.out.println("im1: "+im1.getWidth()+"x"+im1.getHeight());
                //im1 = cropImage(im1, im1.getWidth(), 500);
                System.out.println("im2: "+im2.getWidth()+"x"+im2.getHeight());

                if (im1.getWidth() > 1 && im2.getWidth() > 1 && im2.getHeight() > 1 && im1.getHeight() > 1) {                                
                    return marcalizerCompare(im1, im2);        
                }
                else {
                    System.out.println("One of image dimensions block comparison.");
                }
                

            } catch (IOException ex) {
                Logger.getLogger(TestSelenium.class.getName()).log(Level.SEVERE, null, ex);
            }
            finally {
                tracker.endTrackerMilis("pagelyzer");
            }
            return -1;
        }
        
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                    
            // key is line number 
            // value is the url
            byte[] reference = null;
            int i = 0;
            
            counter ++;
            
            System.out.println(counter+ " "+ value);
            BrowserRep refDriver = null;
            
            for (BrowserRep br : drivers) {
                
                
                // no point in computing image if ref is already null
                if (i > 0 && reference == null) break;
                
                //byte[] image = getScreenShot(br, value.toString());
                byte[] image = getScreenShotWithTimeout(br, value.toString());
                
                saveScreenShot(counter, image, br.desc);
                
                if (i > 0) {                    
                    if (reference != null && image != null) {
                        double score = getScore(reference, image);            
                        //System.out.println("pagelizer score: "+score);
                        res.set(refDriver.desc, br.desc, score);
                        context.write(value, res);                        
                    }
                    else {
                        System.out.println("reference or image are null, not comparing");
                    }
                }
                else {
                    refDriver = br;
                    reference = image;
                }
                i++;
            }
            
            
            if (counter % 100 == 0) {
                tracker.outProgressAll(true);
            }
            
        }             
        
    }
    
    /**
     * Job's reducer definition class
     */
    public static class SCAPEReducer extends Reducer<Text, ResultWritable, Text, Text> {

        public static Text TEXT_NULL = new Text("");
        
        String stringToWrite = null;
        
        static final String XML_DIVIDER = "------------XML------------";
        static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
        //static final String XML_FOOTER = "</xml>";
        static final String XML_FOOTER = "";
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            
            //context.write(new Text("<xml xml version=\"1.0\" encoding=\"UTF-8\">"), new Text(""));
        }

        
        
        private String getXMLRepresentation(String key, ResultWritable res) {
            StringBuilder sb = new StringBuilder();
            
            sb.append("<comparisonResult baseurl=\""+key+"\">");
            sb.append("<browser_desc>");
            
                sb.append(res.getBrowser1());
                
            sb.append("</browser_desc>");
            sb.append("<browser_id>");
            
                sb.append("not used");
                
            sb.append("</browser_id>");            
            sb.append("<confronter>");            
                sb.append("<browser_desc>");
                
                    sb.append(res.getBrowser2());
                    
                sb.append("</browser_desc>");
                sb.append("<browser_id>");
                
                    sb.append("not used");
                    
                sb.append("</browser_id>");

                sb.append("<score>");
                
                    sb.append(res.getScore());
                    
                sb.append("</score>");
            sb.append("</confronter>");
            
            sb.append("</comparisonResult>");
            
            return sb.toString();
            
            
        }
        
        
        @Override
        protected void reduce(Text key, Iterable<ResultWritable> values, Context context) throws IOException, InterruptedException {
            
            StringBuilder sb = new StringBuilder();
            for (ResultWritable res : values) {            
                
                sb.append(getXMLRepresentation(key.toString(), res));                           
            
                // delay the writing by one, so we can catch the last in the cleanup 
                // phase and dont add the xml divider
                if (stringToWrite != null) {
                    context.write(TEXT_NULL, new Text(stringToWrite+"\n"+XML_DIVIDER));

                }
                stringToWrite = sb.toString();                
            }
                        
            //context.write(TEXT_NULL, new Text(sb.toString()));
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            
            if (stringToWrite != null) {
                context.write(TEXT_NULL, new Text(stringToWrite));
            }
                        
            //context.write(new Text("</xml>"), new Text(""));
        }
    
    }
    
    /**
     * Class encapsulating the comparison result
     */
    public static class ResultWritable implements Writable {

        //Text url ;
        String browser1;
        String browser2;
        double score;
        
        @Override
        public void write(DataOutput out) throws IOException {
            //out.writeUTF(url.toString());
            Text.writeString(out, browser1);
            //out.writeUTF(browser1);
            Text.writeString(out, browser2);
            //out.writeUTF(browser2);
            out.writeDouble(score);
            
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            //url.readFields(in);
            
            browser1 = Text.readString(in);
            browser2 = Text.readString(in);
            //browser1 = in.readUTF();
            //browser2 = in.readUTF();
            //DoubleWritable f = new DoubleWritable();
            //f.readFields(in);
            //score = f.get();
            score = in.readDouble();
            
            //System.out.println("this: "+this);
        }
        
        public void set (String b1, String b2, double score) {
            browser1 = b1;
            browser2 = b2;
            this.score = score;            
        }

        public String getBrowser1() {
            return browser1;
        }

        public String getBrowser2() {
            return browser2;
        }

        public double getScore() {
            return score;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            
            sb.append(browser1).append(" ").append(browser2).append(": ").append(score);
            
            return sb.toString();
            
        }
        
    }
    
    /**
     * Take the Jobs output and repack it into one big zip
     * @param zipFilePath
     * @param dir
     * @param f
     * @param conf
     * @throws IOException 
     */
    public void packZipHDFSFiles(Path zipFilePath, Path dir, PathFilter f, Configuration conf) throws IOException {
        
        FileSystem fs = FileSystem.get(conf);
        
        System.out.println("Packaging to " + zipFilePath);
        
        
        HdfsFile zipFile = new HdfsFile(fs, zipFilePath, true, false);
        
        ZipOutputStream zipOut = new ZipOutputStream(zipFile.getOutputStream());
        zipOut.setLevel(Deflater.DEFAULT_COMPRESSION);
        
        
        int counter = 1;
        for (FileStatus file : fs.listStatus(dir, f)) {
            System.out.println("Result file: "+file.getPath());
            HdfsFile inFile = new HdfsFile(fs, file.getPath());
            
            counter = zipResultFileStream(zipOut, inFile.getInputStream(), counter);
            //counter ++;
            
        }
       
        zipOut.flush();
        zipOut.close();
        System.out.println("Done");
    }
        
    private static int zipResultFileStream(ZipOutputStream zos, InputStream is, int counter) throws IOException
    {                        
        LineReader lineReader = new LineReader(is, 4096);
                
        zos.putNextEntry(new ZipEntry("result-"+counter+".xml"));        
        Text line = new Text();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(SCAPEReducer.XML_HEADER.getBytes());                
        while (lineReader.readLine(line) > 0) {
            if (line.toString().equals(SCAPEReducer.XML_DIVIDER)) {
                // write end
                baos.write(SCAPEReducer.XML_FOOTER.getBytes());
                baos.writeTo(zos);
                zos.closeEntry();
                
                counter++;
                baos = new ByteArrayOutputStream();
                zos.putNextEntry(new ZipEntry("result-"+counter+".xml"));
                // write start
                baos.write(SCAPEReducer.XML_HEADER.getBytes());
            }
            else {
                baos.write(line.toString().getBytes());
                baos.write("\n".getBytes());
            }
        }
        
        // write end
        baos.write(SCAPEReducer.XML_FOOTER.getBytes());
        baos.writeTo(zos);
        zos.closeEntry();
        
        is.close();
                
        return counter;
    }
    
    
    
    
    
    /**
     * Start the job w.r.t. the input parameters
     * @param args Input parameters
     * @throws Exception 
     */
    public void processInput(String[] args) throws Exception {        
        Option help = new Option("help", "print this message");
        Option browsersOpt = OptionBuilder.withArgName("browsers").hasArg().withDescription("list of browsers, at least two, e.g. -browser b1 -browser b2").create("browser");
        //Option linesOpt = OptionBuilder.withArgName("lines").hasArg().withDescription("number of lines to process per split").create("lines");
        
        Option inputOpt = OptionBuilder.withArgName("Input path").hasArg().withDescription("Path input").create("input");
        Option outputOpt = OptionBuilder.withArgName("output path").hasArg().withDescription("Path output").create("output");
        Option definitionOpt = OptionBuilder.withArgName("definition").hasArg().withDescription("marcalizer definition file path").create("definition");
        
        
        
        Options options = new Options();        
        options.addOption(help);
        options.addOption(browsersOpt);
        //options.addOption(linesOpt);
        options.addOption(inputOpt);
        options.addOption(outputOpt);
        options.addOption(definitionOpt);
        
        
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);
        
        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("PageRenderCompareMapRed", options);
            System.exit(1);
        }
        
        
        
        String[] browsers = null;
        if (cmd.hasOption(browsersOpt.getOpt())) {
            browsers = cmd.getOptionValues(browsersOpt.getOpt());
            
            System.out.print("browsers: ");
            Utils.outArray(browsers);
            
            if (browsers.length < 2) {
                System.out.println("At least two browsers need to be specified.");
                System.exit(1);
            }
            else {
                conf.setStrings(BROWSERS_REF, browsers);
            }
        }
                
        //String optionValue = cmd.getOptionValue(linesOpt.getOpt(), "100");
        //numLines = Integer.parseInt(optionValue);
        
        String inputVal = cmd.getOptionValue(inputOpt.getOpt());
        String outputVal = cmd.getOptionValue(outputOpt.getOpt());
        
        String definition = cmd.getOptionValue(definitionOpt.getOpt());
        
        conf.set(DEFINITION_REF, definition);
        
        Job job = configJob(conf);
        job.setJarByClass(job.getMapperClass());
        TextInputFormat.setInputPaths(job, new Path(inputVal));
        TextOutputFormat.setOutputPath(job, new Path(outputVal));        
        
        try {
            HdfsDir outputDir = new HdfsDir(FileSystem.get(conf), new Path(outputVal));
            outputDir.delete();
        }
        catch (IOException ex) {
            // if dir does not exist, do nothing            
        }
        
        
        boolean finishedOK = job.waitForCompletion(true);
        
        if (finishedOK) {
            
            //packZipHDFSFiles(new Path("/user/stan/browser_comaparison/", "result.zip"), 
//                new Path("/user/stan/browser_comaparison/"), getResultFilter(), getConf());
            packZipHDFSFiles(new Path(outputVal, "result.zip"), 
                new Path(outputVal), getResultFilter(), getConf());
        }
        
    }
    
    /**
     * Setup the basics for the job
     * @param conf Configuration
     * @return Configured job
     * @throws IOException C
     */
    public Job configJob(Configuration conf) throws IOException {
        Job job = new Job(conf);
        
        //job.
        job.setMapperClass(SCAPEMapper.class);
        job.setReducerClass(SCAPEReducer.class);
        
        job.setMapOutputKeyClass(Text.class);        
        job.setMapOutputValueClass(ResultWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
                
        //job.setInputFormatClass(NLineInputFormat.class);                
        //NLineInputFormat.setNumLinesPerSplit(job, numLines);
        job.setInputFormatClass(TextInputFormat.class);                
        
        
        return job;
    }

    /**
     * Get configuration
     * @return Configuration
     */
    public Configuration getConf() {
        return conf;
    }
    
    /**
     * Get the file filter for the files containing result from the MR job
     * @return 
     */
    static PathFilter getResultFilter() {
        PathFilter pf = new PathFilter() {

            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith("part-r-");
            }            
        };
        return pf;                
    }
        
    
    
    public static void main(String[] args) throws Exception {
        
        PageRenderCompareMapRed ps = new PageRenderCompareMapRed();
        
        ps.processInput(args);
                
        //ps.packZipHDFSFiles(new Path("/user/stan/browsershots-13000/", "result.zip"), 
        //        new Path("/user/stan/browsershots-13000/"), getResultFilter(), ps.getConf());
        
        ps.packZipHDFSFiles(new Path("/tmp/", "result.zip"), 
                new Path("/tmp/"), getResultFilter(), ps.getConf());
        
    }
    
}
