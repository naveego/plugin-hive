namespace PluginHive.API.Utility
{
    public static partial class Utility
    {
        public static string GetSafeString(string unsafeString, string escapeChar = "'", string replaceString = "''")
        {
            return unsafeString.Replace(escapeChar, replaceString);
        }
    }
}