using System;
using System.Security.Cryptography;
using System.Text;

namespace EncryptionLibrary
{
    public sealed class AesEncryption
    {
        private static readonly Lazy<AesEncryption> _instance =
            new Lazy<AesEncryption>(() => new AesEncryption());

        private readonly byte[] _key;
        private readonly byte[] _salt = Encoding.UTF8.GetBytes("static-salt-value");
		
	    private const int KeySize = 32; // 256 bits
        private const int SaltSize = 16;
        private const int NonceSize = 12;
        private const int TagSize = 16;
        private const int TenThousandIterations = 10000;

        private AesEncryption()
        {
            using (var keyDerivation = new Rfc2898DeriveBytes("YourSecretKey", _salt, TenThousandIterations))
            {
                _key = keyDerivation.GetBytes(KeySize); // AES-256 Key
            }
        }

        public static AesEncryption Instance => _instance.Value;
		
        public string Encrypt(string plainText)
        {
            if (string.IsNullOrEmpty(plainText)) throw new ArgumentNullException(nameof(plainText));

            using (var aes = new AesGcm(_key))
            {
                byte[] iv = new byte[12]; // GCM IV
                RandomNumberGenerator.Fill(iv);
                
                byte[] plaintextBytes = Encoding.UTF8.GetBytes(plainText);
                byte[] ciphertext = new byte[plaintextBytes.Length];
                byte[] tag = new byte[TagSize]; // Authentication Tag

                aes.Encrypt(iv, plaintextBytes, ciphertext, tag);

                byte[] encryptedData = new byte[iv.Length + ciphertext.Length + tag.Length];
                Buffer.BlockCopy(iv, 0, encryptedData, 0, iv.Length);
                Buffer.BlockCopy(ciphertext, 0, encryptedData, iv.Length, ciphertext.Length);
                Buffer.BlockCopy(tag, 0, encryptedData, iv.Length + ciphertext.Length, tag.Length);

                return Convert.ToBase64String(encryptedData);
            }
        }

        public string Decrypt(string cipherText)
        {
            if (string.IsNullOrEmpty(cipherText)) throw new ArgumentNullException(nameof(cipherText));

            byte[] encryptedBytes = Convert.FromBase64String(cipherText);
            byte[] iv = new byte[12];
            byte[] tag = new byte[16];
            byte[] ciphertext = new byte[encryptedBytes.Length - iv.Length - tag.Length];

            Buffer.BlockCopy(encryptedBytes, 0, iv, 0, iv.Length);
            Buffer.BlockCopy(encryptedBytes, iv.Length, ciphertext, 0, ciphertext.Length);
            Buffer.BlockCopy(encryptedBytes, iv.Length + ciphertext.Length, tag, 0, tag.Length);

            using (var aes = new AesGcm(_key))
            {
                byte[] decryptedData = new byte[ciphertext.Length];
                aes.Decrypt(iv, ciphertext, tag, decryptedData);

                return Encoding.UTF8.GetString(decryptedData);
            }
        }
    }
}

using NUnit.Framework;
using EncryptionLibrary;
using System;

namespace EncryptionLibraryTests
{
    [TestFixture]
    public class AesEncryptionTests
    {
        [Test]
        public void EncryptionDecryption_ShouldReturnOriginalText()
        {
            string originalText = "MySecurePassword";
            string encryptedText = AesEncryption.Instance.Encrypt(originalText);
            string decryptedText = AesEncryption.Instance.Decrypt(encryptedText);

            Assert.AreEqual(originalText, decryptedText);
        }

        [Test]
        public void Encrypt_NullInput_ShouldThrowException()
        {
            Assert.Throws<ArgumentNullException>(() => AesEncryption.Instance.Encrypt(null));
        }

        [Test]
        public void Encrypt_EmptyInput_ShouldThrowException()
        {
            Assert.Throws<ArgumentNullException>(() => AesEncryption.Instance.Encrypt(""));
        }

        [Test]
        public void Decrypt_NullInput_ShouldThrowException()
        {
            Assert.Throws<ArgumentNullException>(() => AesEncryption.Instance.Decrypt(null));
        }

        [Test]
        public void Decrypt_InvalidCipherText_ShouldThrowException()
        {
            string invalidCipher = "InvalidBase64String";
            Assert.Throws<FormatException>(() => AesEncryption.Instance.Decrypt(invalidCipher));
        }
    }
}
