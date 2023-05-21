/*******************************************************************************
 * -----------------------------------------------------------------------------
 * <br>
 * <p><b>Copyright (c) 2006 InteQ Corporation. All Rights Reserved.</b>
 * <br>
 * <br>
 * This SOURCE CODE FILE, which has been provided by InteQ Corporation as part
 * of a InteQ Corporation product for use ONLY by licensed users of the product,
 * includes CONFIDENTIAL and PROPRIETARY information of InteQ Corporation.<br>
 * <br>
 * USE OF THIS SOFTWARE IS GOVERNED BY THE TERMS AND CONDITIONS
 * OF THE LICENSE STATEMENT AND LIMITED WARRANTY FURNISHED WITH
 * THE PRODUCT.<br>
 * <br>
 * </p>
 * -----------------------------------------------------------------------------
 * <br>
 * <br>
 ******************************************************************************/
package org.nextlevel.es.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
//import sun.misc.BASE64Encoder;
import java.util.Base64;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

/*******************************************************************************
 * <p>This class is a utility class to perform cryptographic hash-operations to
 * generate a digital fingerprint of a message. To be able to use the
 * cryptographic operations, an additonal security provider like
 * <a href="http://www.bouncycastle.org/">BouncyCastle</a> has to be installed.
 * </p><br>
 *
 * @author  Martin Sewering
 * @version $Revision: 1.1.1.1 $
 * @since   Platform 0.1
 *
 * @see Facade
 * @see EncryptionHelper
 ******************************************************************************/
public class Hash
{
    /**
     * Reference to alerter instance for logging platform/application alerts
     * used by monitoring systems.
     */
    //private static Alert alerter = Alert.getInstance();
    
    /**
     * Default Hash algorithm to te used for generating digital footprint.
     */
    protected String algorithm = "SHA1";;
    
    /**
     * <p>Creates a new SHA1-Hash algorithm instance.</p>
     */
    public Hash()
    {
        Security.addProvider( new BouncyCastleProvider() );
    }
    
    /**
     * <p>Creates a new hash algorithm instance.</p><br>
     *
     * @param algorithm The name of the hash algorithm to be created.
     */
    public Hash(String algorithm)
    {
        this(); //to ensure Security Provider is added as defined in default constructor
        this.algorithm = algorithm;
    }
    
    /**
     * <p>Generates the digest (the digital fingerprint) of a message.</p><br>
     *
     * @param msg Message for which the fingerprint shall be generated.
     * @return The digest of parameter msg.
     */
    public byte[] generateDigest(byte[] msg)
    throws CryptographyException
    {
        MessageDigest md;
        byte[] digest = null;
        
        try
        {
            md = MessageDigest.getInstance(algorithm);
            md.update(msg);
            digest = md.digest();
        }
        catch (NoSuchAlgorithmException nsae)
        {
            throw new CryptographyException("Failed to generate digital fingerprint for the input binary message.", nsae);
        }
        
        return digest;
    }
    
    /**
     * <p>Generates the digest (the digital fingerprint) of a message.</p><br>
     *
     * @param msg Message for which the fingerprint shall be generated.
     * @return The digest of parameter msg as Base64 encoded string.
     *
     * @throws com.inteqnet.platform.exceptions.CryptographyException
     */
    public String generateDigest(String msg)
    throws CryptographyException
    {
        
        MessageDigest md;
        byte[] digest = null;
        
        try
        {
            md = MessageDigest.getInstance(algorithm);
            md.update(msg.getBytes());
            digest = md.digest();
        }
        catch (NoSuchAlgorithmException nsae)
        {
            throw new CryptographyException("Failed to generate digital fingerprint for the input text message.",nsae);
        }
        
        //BASE64Encoder encoder = new BASE64Encoder();
        //String result = encoder.encode(digest);
        
        String result = Base64.getEncoder().encodeToString(digest);
        
        return result;
    }
    
    /**
     * <p>Returns the name of the hash algorithm.</p><br>
     *
     * @return The name of the currently used hash algorithm.
     */
    public String getAlgorithm()
    {
        return algorithm;
    }
    
    /**
     * <p>Sets the passed hash algorithm name as the current hash algorithm.</p>
     * <br>
     *
     * @param algorithm Name of the new hash algorithm.
     */
    public void setAlgorithm(String algorithm)
    {
        this.algorithm = algorithm;
    }
}
