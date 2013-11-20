/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.internetmemory.browsershots;

import Taverna.ScapeTest;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.ImageIO;

import net.internetmemory.utils.TrackerNumber;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.Platform;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.remote.Augmenter;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.remote.UnreachableBrowserException;

/**
 *
 * @author barton
 */
public class TestSelenium {
    
	private static final long MAX_WAIT_S = 25;
	
    //Selenium seleniumHub;

    TrackerNumber tracker = new TrackerNumber();
    
    WebDriver driver;
    
    WebDriver driverOpera;
    
    List<String> urls = new ArrayList<String>();
    
    String pathToOutputDir;
    private boolean marcalyzerInitialize = false;
    ScapeTest sc;
    
    BufferedImage lastImage = null;
    
    public TestSelenium() {
        //this.seleniumHub = new DefaultSelenium("im1a5", 4444, "*firefox", "http://www.google.com");
        driver = initWebDriver();
        
        driverOpera = initOperaDriver();
        //driverOpera = initChromeDriver();
        
        initPagelyzer("");
    }
    
    
    public void initPagelyzer(String scFile) {
        marcalyzerInitialize = true;
        sc= new ScapeTest();
        
        if (scFile.isEmpty()) {            
            scFile = "/Users/barton/z-space/implementation/MarcAlizer/exemple/ex2.xml";
            File f = new File(scFile);
            if (!f.exists())
                scFile = "/0/stanislav/MarcAlizer/exemple/ex2.xml";
        }
        
        try {
            sc.init(new File(scFile));
        }
        catch (NullPointerException ex) {
            sc.init(new File("/0/stanislav/MarcAlizer/exemple/ex2.xml"));
        }
        System.out.println("pagelizer initialized.");
    }
    
    
    public void testSelenium(String pathToUrlFile, String pathToOutputDir) {
        
        this.pathToOutputDir  = pathToOutputDir;
        
        
        List<String> urls = new ArrayList();
            
        urls.add("http://www.idnes.cz");
        urls.add("http://www.mobilmania.cz");
        urls.add("http://www.fujirumors.com");
        urls.add("http://wsj.com");        
        
        try {
            urls = readInFileOfURLs(pathToUrlFile);
            for (int i = 44; i < urls.size(); i++) {
                String url = urls.get(i);
                try {
                	//testUrl(url, i);
                    testUrlNew(url, i);
                } catch (Throwable e) {
                	System.out.println("ERROR: Could not load " + url);
                	System.out.println(e.getMessage());
                }
            }
            tracker.outProgressAll();
            
        } 
        catch (FileNotFoundException ex) {
            Logger.getLogger(TestSelenium.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(TestSelenium.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            
        }
        
    }
    
    public WebDriver initOperaDriver() {
        DesiredCapabilities capability = DesiredCapabilities.opera();
        capability.setPlatform(Platform.LINUX);
        return initWebDriver(capability);
    }
    
    public WebDriver initChromeDriver() {
        DesiredCapabilities capability = DesiredCapabilities.chrome();
        capability.setPlatform(Platform.LINUX);
        //capability.setCapability("webdriver.chrome.driver", "/usr/lib/google-chrome");
        //capability.setCapability("chrome.binary", "/usr/lib/google-chrome");        
        //System.setProperty("webdriver.chrome.driver", "/usr/lib/google-chrome");
        return initWebDriver(capability);
    }
    
    public WebDriver initWebDriver() {        
        DesiredCapabilities capability = DesiredCapabilities.firefox();
        capability.setPlatform(Platform.LINUX);
        return initWebDriver(capability);
    }
    
    public WebDriver initWebDriver(DesiredCapabilities capability) {
        tracker.startTrackerMilis("getDriver");        
        
        WebDriver driver;
        
        try {
            //driver = new RemoteWebDriver(new URL("http://im1a10:8090/wd/hub"), capability);
            driver = new RemoteWebDriver(new URL("http://im1a10.internetmemory.org:5550/wd/hub"), capability);
        } catch (MalformedURLException e) {
                throw new RuntimeException("Invalid Selenium driver URL", e);
        }        
        driver = new Augmenter().augment(driver);
        driver.manage().timeouts().pageLoadTimeout(MAX_WAIT_S, TimeUnit.SECONDS);        
        driver.manage().timeouts().implicitlyWait(MAX_WAIT_S, TimeUnit.SECONDS);
        tracker.endTrackerMilis("getDriver");
        return driver;
    }
    
    
    public double marcalizerCompare(BufferedImage im1, BufferedImage im2) {
        if (marcalyzerInitialize == false) {
            initPagelyzer("");
        }
        double score = sc.run(im1, im2);
        return score;
    }
    
    
    private BufferedImage cropImage(BufferedImage src, int width, int height) {
        BufferedImage dest = src.getSubimage(0, 0, width, height);
        return dest; 
    }
    
    
    
    public byte[] getScreenShot(WebDriver driver, String url) {
            File screenshot = null;
            System.out.println("getting image using driver: "+driver);
            byte[] screenshotBytes = null;
            try {
                tracker.startTrackerMilis("getPage");
                driver.get(url);
                String title = driver.getTitle();

                System.out.println("title: "+title);
                tracker.endTrackerMilis("getPage");

                tracker.startTrackerMilis("takeSnapshot");
                    //screenshot = ((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
                screenshotBytes = ((TakesScreenshot)driver).getScreenshotAs(OutputType.BYTES);
                tracker.endTrackerMilis("takeSnapshot");

                //if (screenshot != null ) {
                    tracker.addSingleValue("shotSize", screenshot != null?screenshot.length():screenshotBytes.length);
                //}                    
            } catch(UnreachableBrowserException e) {
                System.out.println("ERROR: Could not load " + url);
                System.out.println("Trying to reinitialize browser");
                driver.close();
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                } catch (InterruptedException e2) {
                }

                //driver = initWebDriver();
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
    
    
    public double testPagelyzer(byte[] image1, byte[] image2) {
        tracker.startTrackerMilis("pagelyzer");
        BufferedImage im1 = null;
        BufferedImage im2 = null;
        try {
            im1 = ImageIO.read(new ByteArrayInputStream(image1));
            
            System.out.println(im1.getWidth()+"x"+im1.getHeight());
            im1 = cropImage(im1, im1.getWidth(), 500);
            System.out.println(im1.getWidth()+"x"+im1.getHeight());
            
            if (lastImage != null) {
                im2 = lastImage;
                lastImage = im1;
                return marcalizerCompare(im1, im2);        
            }            
            lastImage = im1;
            return -1;            
            
        } catch (IOException ex) {
            Logger.getLogger(TestSelenium.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            tracker.endTrackerMilis("pagelyzer");
        }
        return -1;
    }
    
    public double testPagelyzer2(byte[] image1, byte[] image2) {
        tracker.startTrackerMilis("pagelyzer");
        BufferedImage im1 = null;
        BufferedImage im2 = null;
        try {
            im1 = ImageIO.read(new ByteArrayInputStream(image1));
            im2 = ImageIO.read(new ByteArrayInputStream(image1));
            
            System.out.println(im1.getWidth()+"x"+im1.getHeight());
            //im1 = cropImage(im1, im1.getWidth(), 500);
            System.out.println(im2.getWidth()+"x"+im2.getHeight());
            
            if (im1 != null && im2 != null) {
                return marcalizerCompare(im1, im2);        
            }            
            
        } catch (IOException ex) {
            Logger.getLogger(TestSelenium.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            tracker.endTrackerMilis("pagelyzer");
        }
        return -1;
    }
    
    
    
    public void testUrlNew(String url, int counter) {
        byte[] screenShot = getScreenShot(driver, url);

        //skip if reference is null
        if (screenShot == null) return;
        
        byte[] screenShot2 = getScreenShot(driverOpera, url);
        
        saveScreenShot(counter, screenShot, driver.toString());
        saveScreenShot(counter, screenShot2, driverOpera.toString());
        
        double testPagelyzer = testPagelyzer2(screenShot, screenShot2);
        System.out.println("pagelizer score: "+testPagelyzer);
        
        
    }
    
    public void testUrl(String url, int counter) {
        File screenshot = null;
        byte[] screenshotBytes = null;
        byte[] screenshotBytes2 = null;
        try {
            tracker.startTrackerMilis("getPage");
            driver.get(url);
            String title = driver.getTitle();

            System.out.println("title: "+title);
            tracker.endTrackerMilis("getPage");

            tracker.startTrackerMilis("takeSnapshot");
        	//screenshot = ((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
            screenshotBytes = ((TakesScreenshot)driver).getScreenshotAs(OutputType.BYTES);
            
            saveScreenShot(counter, screenshotBytes, driver.toString());
            
            driverOpera.get(url);
            
            System.out.println(driverOpera);
            
            screenshotBytes2 = ((TakesScreenshot)driverOpera).getScreenshotAs(OutputType.BYTES);
            
            saveScreenShot(counter, screenshotBytes2, driverOpera.toString());
            
            tracker.endTrackerMilis("takeSnapshot");
            
            //double testPagelyzer = testPagelyzer(screenshotBytes, screenshotBytes);
            double testPagelyzer = testPagelyzer2(screenshotBytes, screenshotBytes2);
            System.out.println("pagelizer score: "+testPagelyzer);
                       
            
            //if (screenshot != null ) {
                tracker.addSingleValue("shotSize", screenshot != null?screenshot.length():screenshotBytes.length);
            //}
        } catch(UnreachableBrowserException e) {
            System.out.println("ERROR: Could not load " + url);
            System.out.println("Trying to reinitialize browser");
            driver.close();
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e2) {
            }
            driver = initWebDriver();
            return;
        } catch (Throwable e) {
            System.out.println("ERROR: Could not load " + url);
            System.out.println(e);
            e.printStackTrace();
            return;
        }
        
        //;
        //byte[] screenshotAsBytes = ((TakesScreenshot)driver).getScreenshotAs(OutputType.BYTES);
        //tracker.addSingleValue("shotSize", screenshotAsBytes.length);
        tracker.endTrackerMilis("takeSnapshot");
        
        if (screenshot != null) {
            try {
                FileUtils.copyFile(screenshot, new File(pathToOutputDir+"/screenshot-"+counter+".png"));
            } catch (FileNotFoundException ex) {
                Logger.getLogger(TestSelenium.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(TestSelenium.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        //System.out.println("screenshot: "+screenshot);

        
    }
    
    
    public void saveScreenShot(int i, byte[] sShot, String browser) {
            
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
    
    
    public void testUrlCrawled(String url, int counter) {
        tracker.startTrackerMilis("getPage");
        driver.get(url);
        
        //driver1.get(url);
        String title = driver.getTitle();

        System.out.println("title: "+title);
        tracker.endTrackerMilis("getPage");

        tracker.startTrackerMilis("takeSnapshot");
        File screenshot = ((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
        tracker.addSingleValue("shotSize", screenshot.length());
        //;
        //byte[] screenshotAsBytes = ((TakesScreenshot)driver).getScreenshotAs(OutputType.BYTES);
        //tracker.addSingleValue("shotSize", screenshotAsBytes.length);
        tracker.endTrackerMilis("takeSnapshot");
        
        try {
            FileUtils.copyFile(screenshot, new File(pathToOutputDir+"/screenshot-"+counter+".png"));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(TestSelenium.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(TestSelenium.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    public static List<String> readInFileOfURLs(String fName) throws FileNotFoundException, IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(fName)));
        List<String> ret = new ArrayList();
        String line = null;
        while ((line = reader.readLine()) != null) {
            if (!line.isEmpty()) {
                ret.add(line);
                System.out.println(line);
            }
        }

        reader.close();

        return ret;

    }
    
    
    public void testMarcComparison(String im1, String im2) throws IOException {
        tracker.startTrackerMilis("readFile");        
        BufferedImage image1 = ImageIO.read(new File(im1));                        
        tracker.endTrackerMilis("readFile");        
        
        image1 = cropImage(image1, image1.getWidth(), 500);
        
        tracker.startTrackerMilis("readFile");        
        BufferedImage image2 = ImageIO.read(new File(im2));        
        tracker.endTrackerMilis("readFile");        
                
        image2 = cropImage(image2, image2.getWidth(), 500);
        for (int i = 0; i < 10; i++) {
        
            tracker.startTrackerMilis("compare");        
            double marcalizerCompare = marcalizerCompare(image1, image2);
            tracker.endTrackerMilis("compare");        
            System.out.println("score: "+marcalizerCompare);

        }
        tracker.outProgressAll();
        
    }
    
    public static void main (String[] args) throws IOException{
        
        TestSelenium ts = new TestSelenium();
        
        //String pathToUrlFile = "/tmp/urls.txt";
        String pathToUrlFile = "/Users/barton/tmp/refaa";
        File f = new File(pathToUrlFile);
        if (!f.exists())
            pathToUrlFile = "/home/stan/refaa";
        
        String pathToOutputDir = "/tmp/scnshts";
        
        if (args.length >= 1) {
            pathToUrlFile = args[0];
        }
        if (args.length == 2) {
            pathToOutputDir = args[1];
        }
        
        ts.testSelenium(pathToUrlFile, pathToOutputDir);
        
        //ts.testMarcComparison(pathToOutputDir+"/screenshot-1.png", pathToOutputDir+"/screenshot-1.png");
        
        
    }
}
