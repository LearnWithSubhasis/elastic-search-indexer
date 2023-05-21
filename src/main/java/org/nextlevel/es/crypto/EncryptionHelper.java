///*******************************************************************************
// * -----------------------------------------------------------------------------
// * <br>
// * <p><b>Copyright (c) 2006 InteQ Corporation. All Rights Reserved.</b>
// * <br>
// * <br>
// * This SOURCE CODE FILE, which has been provided by InteQ Corporation as part
// * of a InteQ Corporation product for use ONLY by licensed users of the product,
// * includes CONFIDENTIAL and PROPRIETARY information of InteQ Corporation.<br>
// * <br>
// * USE OF THIS SOFTWARE IS GOVERNED BY THE TERMS AND CONDITIONS
// * OF THE LICENSE STATEMENT AND LIMITED WARRANTY FURNISHED WITH
// * THE PRODUCT.<br>
// * <br>
// * </p>
// * -----------------------------------------------------------------------------
// * <br>
// * <br>
// * Modification History:
// *
// * Date         UserID          ID       Change Description
// * ----------   --------------  -------- ---------------------------------------
// * 18/05/2011   dshah           144196   Added support for key-based encryption and decryption using
// *                                       Triple DES or DES Encryption schemes
// * 03/04/2012   ashah           156349   Added new methods to do cryptography as per nimsoft.jar.
// ******************************************************************************/
//package org.nextlevel.es.crypto;
//
//import java.io.IOException;
//import java.security.spec.KeySpec;
//
//import javax.crypto.Cipher;
//import javax.crypto.SecretKey;
//import javax.crypto.SecretKeyFactory;
//import javax.crypto.spec.DESKeySpec;
//import javax.crypto.spec.DESedeKeySpec;
//
//import sun.misc.BASE64Decoder;
//import sun.misc.BASE64Encoder;
//
//import com.nimsoft.nimbus.NimException;
//import com.nimsoft.nimbus.NimSecurity;
//
///**
// * <p>This method provides Encryption related helper / convenience methods for
// * encrypting string or comparing the clear text with a digitalized text.</p>
// */
//public class EncryptionHelper
//{
//   /**
//    * <p>Encryption Seed</p>
//    */
//   public static final String SD_SEED = "_nsdgtw";
//   
//   /**
//    * <p>Name of Hash Algorithm used for encryption</p>
//    */
//   public static final String HASH_ALGORITHM = "SHA1";   
//	   
//   /**
//    * <p>Triple DES Encryption Scheme; single-key encryption algorithm</p>
//    */
//   public static final String DESEDE_ENCRYPTION_SCHEME = "DESede";
//
//   /**
//    * <p>DES Encryption Scheme; single-key encryption algorithm</p>
//    */
//   public static final String DES_ENCRYPTION_SCHEME = "DES";   
//   
//   /**
//    * <p>Encrypt/Digitalize the specified text/string and return the
//    * digitalized/encrypted byte array.</p>
//    *
//    * @param clearText Text string to be encrypted/digitalized.
//    * @return Digitalized/Encrypted byte array.
//    *
//    * @throws com.inteqnet.platform.exceptions.CryptographyException
//    */
//   public byte[] encrypt (String clearText)
//   throws CryptographyException
//   {
//      Hash hashInstance = Facade.getInstance ().getHash (HASH_ALGORITHM);
//      byte[] digitalizedText = null;
//      try
//      {
//         digitalizedText = hashInstance.generateDigest (clearText.getBytes ());
//      }
//      catch (CryptographyException ce)
//      {
//         throw ce;
//      }
//      return digitalizedText;
//   }
//
//
//   /**
//    * <p>This method takes a BASE64 encoded string and decodes it.</p>
//    *
//    * @param encodedString BASE64 Encoded String.
//    * @return Decoded String
//    *
//    * @throws java.io.IOException
//    */
//   public String decode (String encodedString)
//   throws IOException
//   {
//      byte[] decodedString = null;
//      try
//      {
//         BASE64Decoder decoder = new BASE64Decoder ();
//         decodedString = decoder.decodeBuffer (encodedString);
//      }
//      catch (IOException ioe)
//      {
//         throw ioe;
//      }
//      return new String (decodedString);
//   }
//
//
//   /**
//    * <p>This method takes clear text/string and encodes it as BASE64 format.</p>
//    *
//    * @param encodedString BASE64 Encoded String.
//    * @return Decoded String
//    */
//   public String encode (String clearText)
//   {
//      String encodedString = null;
//      BASE64Encoder encoder = new BASE64Encoder ();
//      encodedString = encoder.encodeBuffer (clearText.getBytes ());
//      return encodedString;
//   }
//
//
//   /**
//    * <p>Matches the digitalized/encrypted text with the clear text string and
//    * returns a boolean indicating the status.</p>
//    *
//    * @param encText Encrypted byte array to be compared.
//    * @param clearText Clear text string to be compared.
//    * @return Returns true if the Encrypted byte array matches the clear text
//    * string, else returns a false.
//    *
//    * @throws com.inteqnet.platform.exceptions.CryptographyException
//    */
//   public boolean isMatching (byte[] encText, String clearText)
//   throws CryptographyException
//   {
//      Hash hashInstance = Facade.getInstance ().getHash (HASH_ALGORITHM);
//      byte[] generatedHash = null;
//      try
//      {
//         generatedHash = hashInstance.generateDigest (clearText.getBytes ());
//      }
//      catch (CryptographyException ce)
//      {
//         throw ce;
//      }
//      return ByteHandler.isEqual (encText, generatedHash);
//   }
//
//
//   /**
//    * <p>Match two different digitalized/encrypted binary data and
//    * return a boolean indicating the status.</p>
//    *
//    * @param encText1 Encrypted byte array to be compared with encText2.
//    * @param encText2 Encrypted byte array to be compared with encText1.
//    * @return Returns true if both the Encrypted byte arrays are equal, else
//    * returns a false.
//    */
//   public boolean isMatching (byte[] encText1, byte[] encText2)
//   {
//      return ByteHandler.isEqual (encText1, encText2);
//   }
//
//
//   /**
//    * <p>Encrypt the specified text/string with the specified encryption scheme and encryption key and return a encrypted byte array. It is
//    * used to secure the parameters used by the source code when they are stored in external property, configuration files, or the database.</p>
//    *
//    * @param encScheme           The encryption scheme to use
//    * @param encKeySpec          The specification of the key material that constitutes a cryptographic key
//    * @param unencryptedString   The Text string to be encrypted
//    *
//    * @return Encrypted String
//    *
//    * @throws com.inteqnet.platform.exceptions.CryptographyException
//    */
//   public String encrypt (String encScheme, String encKeySpec, String unencryptedString)
//   throws CryptographyException
//   {
//      KeySpec keySpec;
//      SecretKeyFactory keyFactory;
//      Cipher cipher;
//           
//      try
//      {
//         // Constitutes a cryptographic key, if supported encryption scheme is specified
//         byte[] keyAsBytes = encKeySpec.getBytes ("UTF-8");
//         
//         //appending the value of the key to itself till its length becomes 24 otherwise it would
//         //throw exception as the key's length need to be atleast 24(and multiple of 8). This happens just for Windows OS.This block of needs to be added to
//         //both encryption and decryption.
//         String originalKeyToBeAppended = encKeySpec;
//         while(keyAsBytes.length < 24)
//         {
//            encKeySpec+=originalKeyToBeAppended;
//            if(encKeySpec.length()>24)
//            {
//              encKeySpec = encKeySpec.substring(0,24);
//              keyAsBytes = encKeySpec.getBytes ("UTF-8");
//              break;
//            }
//            keyAsBytes = encKeySpec.getBytes ("UTF-8");
//         }
//         
//         if (encScheme.equals (DESEDE_ENCRYPTION_SCHEME))
//         {
//            keySpec = new DESedeKeySpec (keyAsBytes);
//         }
//         else if (encScheme.equals (DES_ENCRYPTION_SCHEME))
//         {
//            keySpec = new DESKeySpec (keyAsBytes);
//         }
//         else
//         {
//            throw new IllegalArgumentException ("Encryption scheme not supported: " + encScheme);
//         }
//
//         // Initialize key factories to convert keys (opaque cryptographic keys of type Key) into key specifications
//         // and cryptographic cipher for encryption and decryption
//         keyFactory = SecretKeyFactory.getInstance (encScheme);
//         cipher = Cipher.getInstance (encScheme);
//
//         // Generate secret (symmetric) key and initialize this cipher with a key for encrypting the passed-in string
//         SecretKey key = keyFactory.generateSecret (keySpec);
//         cipher.init (Cipher.ENCRYPT_MODE, key);
//         byte[] clearText = unencryptedString.getBytes ("UTF-8");
//         byte[] cipherText = cipher.doFinal (clearText);
//
//         // Return the encrypted value encoded as BASE64 format
//         BASE64Encoder base64encoder = new BASE64Encoder ();
//         return base64encoder.encode (cipherText);
//      }
//      catch (Exception e)
//      {
//         throw new CryptographyException (e.getMessage ());
//      }
//   }
//
//
//   /**
//    * <p>Decrypt the specified text/string with the specified encryption scheme and encryption key and return the decrypted text string.</p>
//    *
//    * @param encScheme           The encryption scheme used for encryption
//    * @param encKeySpec          The specification of the key material that constitutes a cryptographic key
//    * @param unencryptedString   The encrypted text string to be decrypted
//    *
//    * @return Encrypted String
//    *
//    * @throws com.inteqnet.platform.exceptions.CryptographyException
//    */
//   public String decrypt (String encScheme, String encKeySpec, String encryptedString) throws CryptographyException
//   {
//      KeySpec keySpec;
//      SecretKeyFactory keyFactory;
//      Cipher cipher;
//      
//      try
//      {
//         // Constitutes a cryptographic key, if supported encryption scheme is specified
//         byte[] keyAsBytes = encKeySpec.getBytes ("UTF-8");
//         //appending the value of the key to itself till its length becomes 24 otherwise it would
//         //throw exception as the key's length need to be atleast 24(and multiple of 8). This happens just for Windows OS.This block of needs to be added to
//         //both encryption and decryption.
//         String originalKeyToBeAppended = encKeySpec;
//         while(keyAsBytes.length < 24)
//         {
//            encKeySpec+=originalKeyToBeAppended;
//            if(encKeySpec.length()>24)
//            {
//              encKeySpec = encKeySpec.substring(0,24);
//              keyAsBytes = encKeySpec.getBytes ("UTF-8");
//              break;
//            }
//            keyAsBytes = encKeySpec.getBytes ("UTF-8");
//         }
//         if (encScheme.equals (DESEDE_ENCRYPTION_SCHEME))
//         {
//            keySpec = new DESedeKeySpec (keyAsBytes);
//         }
//         else if (encScheme.equals (DES_ENCRYPTION_SCHEME))
//         {
//            keySpec = new DESKeySpec (keyAsBytes);
//         }
//         else
//         {
//            throw new IllegalArgumentException ("Encryption scheme not supported: " + encScheme);
//         }
//
//         // Initialize key factories to convert keys (opaque cryptographic keys of type Key) into key specifications
//         // and cryptographic cipher for encryption and decryption
//         keyFactory = SecretKeyFactory.getInstance (encScheme);
//         cipher = Cipher.getInstance (encScheme);
//
//         // Generate secret (symmetric) key, initialize this cipher with a key, and decrypt the encrypted string
//         SecretKey key = keyFactory.generateSecret (keySpec);
//         cipher.init (Cipher.DECRYPT_MODE, key);
//         BASE64Decoder base64decoder = new BASE64Decoder ();
//         byte[] cleartext = base64decoder.decodeBuffer (encryptedString);
//         byte[] ciphertext = cipher.doFinal (cleartext);
//         return bytesToString (ciphertext);
//      }
//      catch (Exception e)
//      {
//         throw new CryptographyException (e.getMessage ());
//      }
//   }
//
//
//   /**
//    * <p>Convert arrays of bytes to a string.</p>
//    *
//    * @param bytes  The UTF-8 encoded byte array to convert
//    *
//    * @return The bytes converted into a string
//    */
//   private static String bytesToString (byte[] bytes)
//   {
//      StringBuilder text = new StringBuilder ();
//      for (int idx = 0; idx < bytes.length; idx++)
//      {
//         text.append ((char) bytes[idx]);
//      }
//      return text.toString ();
//   }
//
//   /**
//    * <p>This method takes clear text/string and encrypts it as per algorithm in nimsoft.jar.</p>
//    *
//    * @param unencryptedString String to encrypt.
//    * @return Encrypted string
//    */
//   public String nsdEncrypt (String unencryptedString) throws CryptographyException
//   {
//      String encryptedString = null;
//      try
//      {
//         encryptedString = new NimSecurity().encrypt(SD_SEED, unencryptedString);
//      }
//      catch (NimException ne)
//      {
//         throw new CryptographyException(ne.getMessage());
//      }
//      return encryptedString;
//   }
//
//   /**
//    * <p>This method takes encrypted text/string and decrypts it as per algorithm in nimsoft.jar.</p>
//    *
//    * @param encryptedString String to decrypt.
//    * @return Decrypted string
//    */
//   public String nsdDecrypt (String encryptedString) throws CryptographyException
//   {
//      String decryptedString = null;
//      try
//      {
//         decryptedString = new NimSecurity().decrypt(SD_SEED, encryptedString);
//      }
//      catch (NimException ne)
//      {
//         throw new CryptographyException(ne.getMessage());
//      }
//      return decryptedString;
//   }
//
//   
//   public static void main(String[] args) throws Exception
//   {
//       
//       String password = "";
//       StringBuilder usageText = new StringBuilder ();
//       usageText.append ("Usage: java <memory options> <classpath> com.nimsoft.deploy.EncryptionHelper [-options] \n");
//       usageText.append ("Followings are the valid options. ");
//       usageText.append ("  -encrypt or -decrypt  <operation>           Operation to be performed. ");
//       usageText.append ("  -  <passwordString>             Actual password to be either encrypted or decrypted.");
//       if (args.length < 2)
//       {
//           System.out.println (usageText.toString ());
//           return;
//       }
//       try
//       {
//           if(args[0].equalsIgnoreCase("encrypt"))
//           {
//             password = (new EncryptionHelper()).nsdEncrypt(args[1]);
//           }else if(args[0].equalsIgnoreCase("decrypt"))
//           {
//                   password = (new EncryptionHelper()).nsdEncrypt(args[1]);
//           }else
//           {
//               System.out.println("Bad argument passed........\n"+usageText.toString ());
//           }
//       } catch (Exception nex)
//       {
//        throw new Exception (nex.getMessage ());
//       }
//       
//       System.out.println(password);
//   }
//}
