using System;
using PluginHive.Helper;
using Xunit;

namespace PluginHiveTest.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateValidTest()
        {
            // setup
            var settings = new Settings
            {
                ConnectionString = "valid"
            };

            // act
            settings.Validate();

            // assert
        }

        [Fact]
        public void ValidateNoConnectionStringTest()
        {
            // setup
            var settings = new Settings
            {
                ConnectionString = null
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The ConnectionString property must be set", e.Message);
        }

        [Fact]
        public void GetConnectionStringTest()
        {
            // setup
            var settings = new Settings
            {
                ConnectionString = "valid"
            };

            // act
            var connString = settings.GetConnectionString();
            var connDbString = settings.GetConnectionString("otherdb");

            // assert
            Assert.Equal("valid", connString);
            Assert.Equal("valid", connDbString);
        }
    }
}