import cn.edu.szu.dataskew.dataproduce.ZipfGenerator;
import cn.edu.szu.dataskew.dataproduce.ZipfGenerator1;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;

public class Test1 {
    public static void main(String[] args) {
        // 定义常用单词列表
        String[] words = {"In the beginning God created the heaven and the earth.",
                "And the earth was without form, and void; and darkness [was] upon the face of the deep. And the Spirit of God moved upon the face of the waters.",
                "And God said, Let there be light: and there was light.",
                "And God saw the light, that [it was] good: and God divided the light from the darkness.",
                "And God called the light Day, and the darkness he called Night. And the evening and the morning were the first day.",
                "And God said, Let there be a firmament in the midst of the waters, and let it divide the waters from the waters.",
                "And God made the firmament, and divided the waters which [were] under the firmament from the waters which [were] above the firmament: and it was so.",
                "And God called the firmament Heaven. And the evening and the morning were the second day.",
                "And God said, Let the waters under the heaven be gathered together unto one place, and let the dry [land] appear: and it was so.",
                "And God called the dry [land] Earth; and the gathering together of the waters called he Seas: and God saw that [it was] good.",
                "And God said, Let the earth bring forth grass, the herb yielding seed, [and] the fruit tree yielding fruit after his kind, whose seed [is] in itself, upon the earth: and it was so.",
                "And the earth brought forth grass, [and] herb yielding seed after his kind, and the tree yielding fruit, whose seed [was] in itself, after his kind: and God saw that [it was] good.",
                "And the evening and the morning were the third day.",
                "And God said, Let there be lights in the firmament of the heaven to divide the day from the night; and let them be for signs, and for seasons, and for days, and years:",
                "And let them be for lights in the firmament of the heaven to give light upon the earth: and it was so.",
                "And God made two great lights; the greater light to rule the day, and the lesser light to rule the night: [he made] the stars also.",
                "And God set them in the firmament of the heaven to give light upon the earth, ",
                "And to rule over the day and over the night, and to divide the light from the darkness: and God saw that [it was] good.",
                "And the evening and the morning were the fourth day.",
                "And God said, Let the waters bring forth abundantly the moving creature that hath life, and fowl [that] may fly above the earth in the open firmament of heaven.",
                "And God created great whales, and every living creature that moveth, which the waters brought forth abundantly, after their kind, and every winged fowl after his kind: and God saw that [it was] good.",
                "And God blessed them, saying, Be fruitful, and multiply, and fill the waters in the seas, and let fowl multiply in the earth.",
                "And the evening and the morning were the fifth day.",
                "And God said, Let the earth bring forth the living creature after his kind, cattle, and creeping thing, and beast of the earth after his kind: and it was so.",
                "And God made the beast of the earth after his kind, and cattle after their kind, and every thing that creepeth upon the earth after his kind: and God saw that [it was] good.",
                "And God said, Let us make man in our image, after our likeness: and let them have dominion over the fish of the sea, and over the fowl of the air, and over the cattle, and over all the earth, and over every creeping thing that creepeth upon the earth.",
                "So God created man in his [own] image, in the image of God created he him; male and female created he them.",
                "And God blessed them, and God said unto them, Be fruitful, and multiply, and replenish the earth, and subdue it: and have dominion over the fish of the sea, and over the fowl of the air, and over every living thing that moveth upon the earth.",
                "And God said, Behold, I have given you every herb bearing seed, which [is] upon the face of all the earth, and every tree, in the which [is] the fruit of a tree yielding seed; to you it shall be for meat.",
                "And to every beast of the earth, and to every fowl of the air, and to every thing that creepeth upon the earth, wherein [there is] life, [I have given] every green herb for meat: and it was so.",
                "And God saw every thing that he had made, and, behold, [it was] very good. And the evening and the morning were the sixth day.",
                "Thus the heavens and the earth were finished, and all the host of them.",
                "And on the seventh day God ended his work which he had made; and he rested on the seventh day from all his work which he had made.",
                "And God blessed the seventh day, and sanctified it: because that in it he had rested from all his work which God created and made.",
                "These [are] the generations of the heavens and of the earth when they were created, in the day that the LORD God made the earth and the heavens,",
                "And every plant of the field before it was in the earth, and every herb of the field before it grew: for the LORD God had not caused it to rain upon the earth, and [there was] not a man to till the ground.",
                "But there went up a mist from the earth, and watered the whole face of the ground.",
                "And the LORD God formed man [of] the dust of the ground, and breathed into his nostrils the breath of life; and man became a living soul.",
                "And the LORD God planted a garden eastward in Eden; and there he put the man whom he had formed.",
                "And out of the ground made the LORD God to grow every tree that is pleasant to the sight, and good for food; the tree of life also in the midst of the garden, and the tree of knowledge of good and evil.",
                "And a river went out of Eden to water the garden; and from thence it was parted, and became into four heads.",
                "The name of the first [is] Pison: that [is] it which compasseth the whole land of Havilah, where [there is] gold;",
                "And the gold of that land [is] good: there [is] bdellium and the onyx stone.",
                "And the name of the second river [is] Gihon: the same [is] it that compasseth the whole land of Ethiopia.",
                "And the name of the third river [is] Hiddekel: that [is] it which goeth toward the east of Assyria. And the fourth river [is] Euphrates.",
                "And the LORD God took the man, and put him into the garden of Eden to dress it and to keep it.",
                "And the LORD God commanded the man, saying, Of every tree of the garden thou mayest freely eat:",
                "But of the tree of the knowledge of good and evil, thou shalt not eat of it: for in the day that thou eatest thereof thou shalt surely die.",
                "And the LORD God said, [It is] not good that the man should be alone; I will make him an help meet for him.",
                "And out of the ground the LORD God formed every beast of the field, and every fowl of the air; and brought [them] unto Adam to see what he would call them: and whatsoever Adam called every living creature, that [was] the name thereof.",
                "And Adam gave names to all cattle, and to the fowl of the air, and to every beast of the field; but for Adam there was not found an help meet for him.",
                "And the LORD God caused a deep sleep to fall upon Adam, and he slept: and he took one of his ribs, and closed up the flesh instead thereof;",
                "And the rib, which the LORD God had taken from man, made he a woman, and brought her unto the man.",
                "And Adam said, This [is] now bone of my bones, and flesh of my flesh: she shall be called Woman, because she was taken out of Man.",
                "Therefore shall a man leave his father and his mother, and shall cleave unto his wife: and they shall be one flesh.",
                "And they were both naked, the man and his wife, and were not ashamed."
        };
        ZipfGenerator1 zipfGenerator = new ZipfGenerator1(words, 0.1);
        PrintWriter printWriter = null;
        long targetSizeInBytes = 5L * 1024 * 1024 * 1024;   // 2GB
        try {
            printWriter = new PrintWriter(new FileWriter("input/zip-f/skewness_0.1.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long generatedDataSizeInBytes = 0L;
        while (generatedDataSizeInBytes < targetSizeInBytes) {
            String word = zipfGenerator.next();
            byte[] wordBytes = word.getBytes(StandardCharsets.UTF_8);
            generatedDataSizeInBytes += wordBytes.length;
            printWriter.println(word);
        }
        printWriter.close();
    }
}
