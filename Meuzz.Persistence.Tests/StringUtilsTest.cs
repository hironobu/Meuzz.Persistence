using System;
using Xunit;

namespace Meuzz.Persistence.Tests
{
    public class StringUtilsTest
    {
        [Fact]
        public void TestSnake2Camel()
        {
            Assert.Equal("fooBar", StringUtils.Snake2Camel("foo_bar"));
            Assert.Equal("FooBar", StringUtils.Snake2Camel("foo_bar", true));
            Assert.Equal("fooBar", StringUtils.Snake2Camel("FOO_BAR"));
            Assert.Equal("FooBar", StringUtils.Snake2Camel("FOO_BAR", true));

            Assert.Equal("aFoo", StringUtils.Snake2Camel("a_foo"));
            Assert.Equal("AFoo", StringUtils.Snake2Camel("a_foo", true));

            Assert.Equal("aBC", StringUtils.Snake2Camel("a_b_c"));
            Assert.Equal("ABC", StringUtils.Snake2Camel("a_b_c", true));

            // without "_"
            Assert.Equal("abc", StringUtils.Snake2Camel("abc"));
            Assert.Equal("Abc", StringUtils.Snake2Camel("abc", true));
            Assert.Equal("foobar", StringUtils.Snake2Camel("fooBar"));

            Assert.Equal("fooBar", StringUtils.Snake2Camel("foo__bar"));
            Assert.Equal("FooBar", StringUtils.Snake2Camel("foo__bar", true));
            Assert.Equal("fooBar", StringUtils.Snake2Camel("FOO__BAR"));
            Assert.Equal("FooBar", StringUtils.Snake2Camel("FOO__BAR", true));

            Assert.Equal("aaBbCc", StringUtils.Snake2Camel("aa___bb__cc"));
            Assert.Equal("AaBbCc", StringUtils.Snake2Camel("aa___bb__cc", true));

            Assert.Equal("", StringUtils.Snake2Camel(""));
            Assert.Equal("", StringUtils.Snake2Camel("", true));

            var ex = Assert.Throws<ArgumentNullException>(() => StringUtils.Snake2Camel(null));
            Assert.Contains("Parameter 's'", ex.Message);
        }

        [Fact]
        public void TestCamel2Snake()
        {
            Assert.Equal("foo_bar", StringUtils.Camel2Snake("fooBar"));
            Assert.Equal("foo_bar", StringUtils.Camel2Snake("FooBar"));
            Assert.Equal("FOO_BAR", StringUtils.Camel2Snake("fooBar", true));
            Assert.Equal("FOO_BAR", StringUtils.Camel2Snake("FooBar", true));

            Assert.Equal("a_b_c", StringUtils.Camel2Snake("ABC"));
            Assert.Equal("A_B_C", StringUtils.Camel2Snake("ABC", true));

            Assert.Equal("", StringUtils.Camel2Snake(""));
            Assert.Equal("", StringUtils.Camel2Snake("", true));

            var ex = Assert.Throws<ArgumentNullException>(() => StringUtils.Camel2Snake(null));
            Assert.Contains("Parameter 's'", ex.Message);
        }
    }
}
