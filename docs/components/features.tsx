import {
  BoltIcon,
  ChartBarIcon,
  ChartBarSquareIcon,
  CodeBracketIcon,
  CurrencyDollarIcon,
  LockClosedIcon
} from "@heroicons/react/24/outline";
import { ComponentProps } from "react";

export type Feature = {
  name: string;
  description: string;
  Icon: (props: ComponentProps<"svg">) => JSX.Element;
  // page: "all" | "home" | "docs";
};

export type Features = Array<Feature>;

const FEATURES: Features = [
  {
    name: "Low-latency",
    description: `Guaranteed by implementing atop of
    [QUIC](https://datatracker.ietf.org/wg/quic/documents/)`,
    Icon: BoltIcon,
  },
  {
    name: "Security",
    description: `TLS v1.3 on every data packet by design.`,
    Icon: LockClosedIcon,
  },
  {
    name: "Open source",
    description: `See Github.`,
    Icon: CodeBracketIcon,
  },
  {
    name: "Geo-distributed Architecture",
    description: `Your code close to your user.`,
    Icon: ChartBarIcon,
  },
  {
    name: "5G/WiFi-6",
    description: `Reliable networking in Celluar/Wireless.`,
    Icon: ChartBarSquareIcon,
  },
  {
    name: "Streaming Serverless",
    description: `Stateful serverless.`,
    Icon: CurrencyDollarIcon,
  },
];

type FeatureProps = {
  feature: Feature;
  // include feature description
  detailed?: boolean;
};

function Feature(props: FeatureProps) {
  const { feature, detailed = false } = props;
  const { Icon, name, description } = feature;

  return (
    <div className="p-10 bg-white shadow-lg rounded-xl dark:bg-opacity-5 ">
      <div>
        <Icon
          className="h-8 w-8 dark:text-white rounded-full p-1.5 dark:bg-white dark:bg-opacity-10 bg-black bg-opacity-5 text-black"
          aria-hidden="true"
        />
      </div>
      <div className="mt-4">
        <h3 className="text-lg font-medium dark:text-white">{name}</h3>
        <p className="mt-2 text-base font-medium text-gray-500 dark:text-gray-400">
          {description}
        </p>
      </div>
    </div>
  );
}

export default function FeatureList({
  detailed = true,
}: {
  detailed?: boolean;
}) {
  return (
    <div className="grid grid-cols-1 mt-12 gap-x-6 gap-y-12 sm:grid-cols-2 lg:mt-16 lg:grid-cols-3 lg:gap-x-8 lg:gap-y-12">
      {FEATURES.map((feature) => (
        <Feature
          key={feature.name.split(" ").join("-")}
          feature={feature}
          detailed
        />
      ))}
    </div>
  );
}