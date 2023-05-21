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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/*******************************************************************************
 * <p>This is a singleton-class for using symmetric cryptography and digital 
 * fingerprints (cryptographic hashes). For each hash algorithm, one instance is 
 * created. This instance can be requested and then used to perform the 
 * cryptographic operations. To be able to use all the algorithms, an additonal 
 * security provider like <a href="http://www.bouncycastle.org/">BouncyCastle</a>
 * has to be installed. The listed algorithms are currently considered to be 
 * secure, they are also free to use / public domain.</p><br>
 * 
 *
 * @author  Martin Sewering
 * @version $Revision: 1.1.1.1 $
 * @since   Platform 0.1
 *
 ******************************************************************************/

public class Facade {
    
    private static Facade facade = null;
    
    /*
    private Map<String, Symmetric> ciphers = new TreeMap<String, Symmetric>();
    */
    
    private final String[] availableHashAlgorithms = new String[] {
                                                                    "RipeMD160",
                                                                    "RipeMD256",
                                                                    "RipeMD320",
                                                                    "SHA1",
                                                                    "SHA224",
                                                                    "SHA256",
                                                                    "SHA256",
                                                                    "SHA384",
                                                                    "SHA512",
                                                                    "Tiger",
                                                                    "Whirlpool" };
    
    private Map<String, Hash> hashes = new TreeMap<String, Hash>();
    
    /**
     * <p>This is a private constructor since it is a singleton class. It 
     * initializes the supported hash algorithms and places them in a Map 
     * object.</p>
     */
    private Facade () {
        /*
        ciphers.put ( "AES",       new AES () );
        ciphers.put ( "Blowfish",  new Blowfish () );
        ciphers.put ( "Serpent",   new Serpent () );
        ciphers.put ( "TwoFish",   new TwoFish () );
        ciphers.put ( "TripleDES", new TripleDES () );
        */
        
        String[] hashesNames = availableHashAlgorithms;
        for (String hashName : hashesNames) {
            hashes.put (hashName, new Hash (hashName));
        }
    }
    
    /**
     * <p>Checks whether the instance of this class is already available and 
     * return it to the caller. If it is not available then it will create a new
     * instance and return to the caller. </p><br>
     *
     * @return Instance of this class.
     */
    public static Facade getInstance () {
        if (facade == null) {
            facade = new Facade ();
        }
        return facade;
    }
    
    /**
     * <p>Returns a specific cipher object(instance of Symmetric) from the 
     * cipher collection based on the cipher name passed to the method. </p><br>
     *
     * @param  name Name of the requested cipher.
     * @return Instance of the cipher, or null if there is no cipher with 
     *         requested name.
     */
    /*
    public Symmetric getCipher (String name) {
        return ciphers.get (name);
    }
    */

    /**
     * <p>Adds a specific cipher object(instance of Symmetric) to the cipher 
     * collection(Map) with the key being the name passed to this method.</p><br>
     *
     * @param name Key of the cipher to be added to the collection.
     * @param name Cipher object to be added to the collection.
     */    
    /*
    public void addCipher (String name, Symmetric cipher) {
        ciphers.put (name, cipher);
    }
    */
    
    /**
     * <p>Returns a specific hash-algorithm based on the name passed to this 
     * method</p><br>
     *
     * @param  name Name of the requested hash-algorithm.
     * @return Instance of the name parameter hash-algorithm, or null if there 
     *         is no hash-algorithm with name parameter.
     */
    public Hash getHash (String name) {
        return hashes.get (name);
    }
    
    /**
     * <p>Adds a hash-algorithm to the available collection of hash algorithms 
     * with the key being the name passed to this method.</p><br>
     *
     * @param name Name of the hash-algorithm to add.
     * @param hash Instance of the Hash object
     */
    public void addHash (String name, Hash hash) {
        hashes.put (name, hash);
    }
    
    /**
     * <p>Returns a set of all available cipher-instances.</p><br>
     *
     * @return Set with the names of all available cipher-instances.
     */
    /*
    public Set<String> getCiphers () {
        return ciphers.keySet ();
    }
    */
    
    /**
     * <p>Returns a set of all available hash-algorithms-instances.</p><br>
     *
     * @return Set with the names of all available hash-algorithms-instances.
     */
    public Set<String> getHashes () {
        return hashes.keySet ();
    }
}
